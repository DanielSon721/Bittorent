import socket
import struct
from typing import Optional, List

class PeerConnection:
    # wraps tcp socket for peer messages

    def __init__(self, ip: str, port: int, info_hash: bytes, peer_id: bytes):
        self.ip = ip
        self.port = port
        self.info_hash = info_hash
        self.peer_id = peer_id

        self.sock: Optional[socket.socket] = None
        self.connected = False

        self.peer_id_remote = None
        self.peer_choking = True
        self.peer_interested = False
        self.am_choking = False
        self.am_interested = False
        self.bitfield: List[bool] = []
        self.remote_bitfield: List[bool] = []

    def connect(self, timeout=5.0) -> bool:
        import socket
        try:
            family = socket.AF_INET6 if ":" in self.ip else socket.AF_INET
            self.sock = socket.socket(family, socket.SOCK_STREAM)
            self.sock.settimeout(timeout)

            if family == socket.AF_INET6:
                self.sock.connect((self.ip, self.port, 0, 0))
            else:
                self.sock.connect((self.ip, self.port))

            self._send_handshake()

            if not self._recv_handshake():
                print(f"Handshake rejected by peer {self.ip}:{self.port}")
                self.close()
                return False

            self.connected = True
            return True

        except Exception as e:
            self.close()
            return False

    @classmethod
    def from_socket(cls, sock, ip, port, info_hash, peer_id):
        peer = cls(ip, port, info_hash, peer_id)
        peer.sock = sock
        peer.connected = True

        if not peer._recv_handshake():
            peer.close()
            raise ConnectionError("Inbound handshake failed")
        peer._send_handshake()
        return peer

    def _send_handshake(self):
        pstr = b"BitTorrent protocol"
        handshake = (
            bytes([19]) + pstr + bytes(8) +
            self.info_hash + self.peer_id
        )
        self.sock.sendall(handshake)

    def _recv_handshake(self) -> bool:
        try:
            pstrlen = self.sock.recv(1)[0]
            pstr = self._recv_exact(pstrlen)
            if pstr != b"BitTorrent protocol":
                return False

            reserved = self._recv_exact(8)
            recv_info = self._recv_exact(20)
            if recv_info != self.info_hash:
                return False

            self.peer_id_remote = self._recv_exact(20)
            return True
        except:
            return False

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
        self.connected = False

    def _send(self, msg_id: int, payload=b""):
        if not self.connected:
            return
        msg = struct.pack("!I", 1 + len(payload)) + bytes([msg_id]) + payload
        self.sock.sendall(msg)

    def send_interested(self):
        self._send(2)

    def send_not_interested(self):
        self._send(3)

    def send_choke(self):
        self._send(0)

    def send_unchoke(self):
        self._send(1)

    def send_have(self, index: int):
        self._send(4, struct.pack("!I", index))

    def send_bitfield(self, bitfield: List[bool]):
        payload = bytes([int("".join("1" if b else "0" for b in bitfield)[i:i+8],2) for i in range(0,len(bitfield),8)])
        self._send(5, payload)

    def send_request(self, index: int, begin: int, length: int):
        payload = struct.pack("!III", index, begin, length)
        self._send(6, payload)

    def send_piece(self, index: int, begin: int, block: bytes):
        self._send(7, struct.pack("!II", index, begin) + block)

    def send_keepalive(self):
        self.sock.sendall(struct.pack("!I",0))

    def receive_message(self, timeout=0.5):
        self.sock.settimeout(timeout)
        try:
            header = self.sock.recv(4)
            if not header:
                return None
            length = struct.unpack("!I", header)[0]
            if length == 0:
                return {"type":"keepalive"}
            msg_id = self.sock.recv(1)[0]
            payload = b""
            if length > 1:
                payload = b""
                while len(payload) < length-1:
                    chunk = self.sock.recv(length-1 - len(payload))
                    if not chunk:
                        break
                    payload += chunk
            return self._decode(msg_id,payload)
        except socket.timeout:
            return None
        except:
            self.close()
            return None
        
    def _recv_exact(self, n: int) -> bytes:
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Socket closed during _recv_exact")
            data += chunk
        return data

    def _decode(self, msg_id, payload):
        match msg_id:
            case 0: return {"type":"choke"}
            case 1: return {"type":"unchoke"}
            case 2: return {"type":"interested"}
            case 3: return {"type":"not_interested"}
            case 4:
                idx = struct.unpack("!I",payload)[0]
                return {"type":"have","piece_index":idx}
            case 5:
                self.bitfield = self._parse_bitfield(payload)
                return {"type":"bitfield","bitfield":self.bitfield}
            case 6:
                i,b,l = struct.unpack("!III",payload)
                return {"type":"request","index":i,"begin":b,"length":l}
            case 7:
                i,b = struct.unpack("!II",payload[:8])
                block = payload[8:]
                return {"type":"piece","index":i,"begin":b,"block":block}
            case 8:
                return {"type":"cancel"}
        return None

    def _parse_bitfield(self, payload: bytes) -> List[bool]:
        bits=""
        for b in payload:
            bits+="{:08b}".format(b)
        return [c=="1" for c in bits]

    def has_piece(self,index:int)->bool:
        return index < len(self.bitfield) and self.bitfield[index] == True
