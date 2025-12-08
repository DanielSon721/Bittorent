# tracker.py
import socket
import ssl
import urllib.parse
from typing import List, Tuple, Optional
import bencodepy
import struct
import random
import time

class TrackerClient:
    # tracker client for http/https/udp
    _UDP_CONNECT_MAGIC = 0x41727101980
    _UDP_CONNECT_ACTION = 0
    _UDP_ANNOUNCE_ACTION = 1

    def __init__(self, announce_url: str, info_hash: bytes, peer_id: bytes, port: int):
        self.announce_url = announce_url
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.port = port
        self._interval = 120

    def _url_encode_bytes(self, data: bytes) -> str:
        return "".join(f"%{b:02X}" for b in data)

    def build_announce_query(self, downloaded=0, left=0, uploaded=0, event: Optional[str] = None) -> str:
        params = {
            "info_hash": self._url_encode_bytes(self.info_hash),
            "peer_id": self._url_encode_bytes(self.peer_id),
            "port": str(self.port),
            "uploaded": str(uploaded),
            "downloaded": str(downloaded),
            "left": str(left),
            "compact": "1",
            "numwant": "200",
        }
        if event:
            params["event"] = event
        return "&".join(f"{k}={v}" for k, v in params.items())

    def announce(self, downloaded=0, left=0, uploaded=0, event: Optional[str] = None) -> List[Tuple[str, int]]:
        parsed = urllib.parse.urlparse(self.announce_url)
        scheme = parsed.scheme.lower()

        if scheme == "http":
            return self._announce_http(parsed, downloaded, left, uploaded, event)
        elif scheme == "https":
            return self._announce_https(parsed, downloaded, left, uploaded, event)
        elif scheme == "udp":
            return self._announce_udp(parsed, downloaded, left, uploaded, event)
        else:
            print(f"[TRACKER] Unsupported tracker protocol: {scheme}")
            return []

    def _announce_http(self, parsed, downloaded, left, uploaded, event):
        return self._announce_http_impl(parsed, downloaded, left, uploaded, event, use_ssl=False)

    def _announce_https(self, parsed, downloaded, left, uploaded, event):
        return self._announce_http_impl(parsed, downloaded, left, uploaded, event, use_ssl=True)

    def _announce_http_impl(self, parsed, downloaded, left, uploaded, event, use_ssl: bool):
        peers = []
        try:
            if parsed.scheme not in ("http", "https"):
                raise ValueError(f"Unsupported tracker protocol: {parsed.scheme}")

            host = parsed.hostname
            port = parsed.port or (443 if use_ssl else 80)
            path = parsed.path or "/"
            if parsed.query:
                path = f"{path}?{parsed.query}"

            query = self.build_announce_query(downloaded, left, uploaded, event)
            request_path = f"{path}&{query}" if "?" in path else f"{path}?{query}"

            request = (
                f"GET {request_path} HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                f"User-Agent: BitTorrentClient/1.0\r\n"
                f"Accept: */*\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            )

            raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_sock.settimeout(10)
            try:
                if use_ssl:
                    ctx = ssl.create_default_context()
                    sock = ctx.wrap_socket(raw_sock, server_hostname=host)
                else:
                    sock = raw_sock

                sock.connect((host, port))
                sock.sendall(request.encode("ascii"))

                response = b""
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    response += chunk
            finally:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    raw_sock.close()
                except Exception:
                    pass

            peers = self._parse_tracker_response(response)
            return peers

        except Exception as e:
            print(f"[TRACKER] HTTP(S) announce failed: {e}")
            return []

    def _announce_udp(self, parsed, downloaded, left, uploaded, event) -> List[Tuple[str,int]]:
        peers = []
        host = parsed.hostname
        port = parsed.port or 80
        addr = (host, port)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5.0)

        try:
            transaction_id = random.randint(0, 2**32 - 1)
            connect_req = struct.pack(">QII", self._UDP_CONNECT_MAGIC, self._UDP_CONNECT_ACTION, transaction_id)

            connect_resp = None
            for attempt in range(3):
                try:
                    sock.sendto(connect_req, addr)
                    data,_ = sock.recvfrom(4096)
                    connect_resp = data
                    break
                except socket.timeout:
                    time.sleep(1+attempt)
                    continue
            if not connect_resp:
                raise TimeoutError("UDP connect request timed out")
            if len(connect_resp) < 16:
                raise ValueError("Invalid UDP connect response length")

            action, resp_tid = struct.unpack_from(">II", connect_resp, 0)
            if resp_tid != transaction_id or action != self._UDP_CONNECT_ACTION:
                raise ValueError("UDP connect response mismatch")

            connection_id = struct.unpack_from(">Q", connect_resp, 8)[0]

            transaction_id = random.randint(0, 2**32 - 1)
            announce_parts = []
            announce_parts.append(struct.pack(">Q", connection_id))
            announce_parts.append(struct.pack(">I", self._UDP_ANNOUNCE_ACTION))
            announce_parts.append(struct.pack(">I", transaction_id))
            announce_parts.append(self.info_hash)
            announce_parts.append(self.peer_id)
            announce_parts.append(struct.pack(">Q", int(downloaded)))
            announce_parts.append(struct.pack(">Q", int(left)))
            announce_parts.append(struct.pack(">Q", int(uploaded)))

            event_map = {"completed":1, "started":2, "stopped":3}
            evt = event_map.get(event,0)
            announce_parts.append(struct.pack(">I", evt))
            announce_parts.append(struct.pack(">I", 0))
            announce_parts.append(struct.pack(">I", random.randint(0,2**32-1)))
            announce_parts.append(struct.pack(">i", -1))
            announce_parts.append(struct.pack(">H", int(self.port)))

            announce_req = b"".join(announce_parts)

            announce_resp=None
            for attempt in range(3):
                try:
                    sock.sendto(announce_req, addr)
                    data,_ = sock.recvfrom(65536)
                    announce_resp=data
                    break
                except socket.timeout:
                    time.sleep(1+attempt)
                    continue
            if not announce_resp:
                raise TimeoutError("UDP announce request timed out")
            if len(announce_resp)<20:
                raise ValueError("Invalid UDP announce response length")

            resp_action, resp_trans = struct.unpack_from(">II", announce_resp, 0)
            if resp_action!=self._UDP_ANNOUNCE_ACTION or resp_trans!=transaction_id:
                raise ValueError("UDP announce response mismatch")

            interval = struct.unpack_from(">I", announce_resp, 8)[0]
            _lee = struct.unpack_from(">I", announce_resp, 12)[0]
            _see = struct.unpack_from(">I", announce_resp, 16)[0]

            peers_blob = announce_resp[20:]
            self._interval=int(interval)

            if len(peers_blob)%6 !=0:
                count=len(peers_blob)//6
            else:
                count=len(peers_blob)//6

            for i in range(count):
                off=i*6
                ip_bytes=peers_blob[off:off+4]
                port_bytes=peers_blob[off+4:off+6]
                ip=".".join(str(b) for b in ip_bytes)
                port=struct.unpack("!H",port_bytes)[0]
                peers.append((ip,port))
            return peers

        except Exception as e:
            print(f"[TRACKER][UDP] Announce failed: {e}")
            return []
        finally:
            try:
                sock.close()
            except Exception:
                pass

    def _parse_tracker_response(self,response:bytes)->List[Tuple[str,int]]:
        try:
            header_end=response.find(b"\r\n\r\n")
            if header_end==-1:
                raise ValueError("Invalid tracker response (no header separator)")
            body=response[header_end+4:]
            tracker_response=bencodepy.decode(body)

            if b"failure reason" in tracker_response:
                reason=tracker_response[b"failure reason"].decode("utf-8",errors="replace")
                raise ValueError(f"Tracker error: {reason}")

            if b"interval" in tracker_response:
                try:
                    self._interval=int(tracker_response[b"interval"])
                except Exception:
                    pass

            if b"peers" not in tracker_response:
                raise ValueError("Tracker response missing 'peers' key")

            peers_data=tracker_response[b"peers"]

            peers: List[Tuple[str,int]] = []

            if isinstance(peers_data,bytes):
                peers.extend(self._parse_compact_peers(peers_data))
            elif isinstance(peers_data,list):
                peers.extend(self._parse_dict_peers(peers_data))
            else:
                raise ValueError("Unknown 'peers' format in tracker response")

            if b"peers6" in tracker_response:
                peers.extend(self._parse_compact_peers6(tracker_response[b"peers6"]))

            return peers

        except Exception as e:
            print(f"[TRACKER] Response parse error: {e}")
            return []

    def _parse_compact_peers(self, peers_data:bytes)->List[Tuple[str,int]]:
        peers=[]
        count=len(peers_data)//6
        for i in range(count):
            off=i*6
            ip_bytes=peers_data[off:off+4]
            port_bytes=peers_data[off+4:off+6]
            ip=".".join(str(b) for b in ip_bytes)
            port=struct.unpack("!H",port_bytes)[0]
            peers.append((ip,port))
        return peers

    def _parse_compact_peers6(self, peers_data: bytes) -> List[Tuple[str,int]]:
        peers=[]
        if len(peers_data) % 18 != 0:
            peers_data = peers_data[: (len(peers_data)//18) * 18]
        count=len(peers_data)//18
        for i in range(count):
            off=i*18
            ip_bytes=peers_data[off:off+16]
            port_bytes=peers_data[off+16:off+18]
            try:
                ip=socket.inet_ntop(socket.AF_INET6, ip_bytes)
            except Exception:
                continue
            port=struct.unpack("!H",port_bytes)[0]
            peers.append((ip,port))
        return peers

    def _parse_dict_peers(self,peers_data:list)->List[Tuple[str,int]]:
        peers=[]
        for p in peers_data:
            try:
                ip=p[b"ip"].decode()
                port=int(p[b"port"])
                peers.append((ip,port))
            except Exception:
                continue
        return peers

    def get_interval(self)->int:
        return int(self._interval)
