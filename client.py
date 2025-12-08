import threading
import time
import random
from typing import List, Optional
from tracker import TrackerClient
from peer_connection import PeerConnection
from piece_manager import PieceManager
from disk_io import DiskIO
import hashlib

class BitTorrentClient:
    def __init__(self, torrent, peer_id: bytes, listen_port: int, output_path: Optional[str] = None, max_peers: int = 20):
        self.torrent = torrent
        self.peer_id = peer_id  # peer id
        self.listen_port = listen_port  # listen port
        self.output_path = output_path or torrent.name  # output path

        # tracker client
        self.tracker = TrackerClient(
            announce_url=torrent.announce,
            info_hash=torrent.info_hash,
            peer_id=peer_id,
            port=listen_port,
        )

        self.piece_manager = PieceManager(torrent)  # piece state
        self.disk = DiskIO(self.output_path, torrent.length)  # disk writer
        self.peers: List[PeerConnection] = []  # active peers

        # control flags
        self._stop = False
        self._lock = threading.Lock()
        self._peer_threads: List[threading.Thread] = []
        self._monitor_thread: Optional[threading.Thread] = None
        self._reannounce_timer: Optional[threading.Timer] = None

        # stats
        self.downloaded = 0
        self.uploaded = 0
        self.start_time = None

        # limits
        self.max_peers = max_peers
        self.connect_timeout = 5.0

    # main entry
    def run(self):
        print(f"Starting BitTorrent client for: {self.torrent.name}")
        print(f"Total size: {self.torrent.length} bytes ({self.torrent.num_pieces()} pieces)")
        self.start_time = time.time()

        try:
            # announce to trackers
            print("\n[TRACKER] Announcing to trackers (started)...")
            left = max(0, self.torrent.length - self.downloaded)
            peer_list = []

            for tracker_url in self.torrent.announce_list:
                self.tracker.announce_url = tracker_url
                try:
                    peers = self.tracker.announce(downloaded=self.downloaded, left=left, uploaded=self.uploaded, event="started")
                    peer_list.extend(peers)
                except Exception as e:
                    print(f"[TRACKER] Failed to announce to {tracker_url}: {e}")
            
            # remove duplicates
            peer_list = list(set(peer_list))
            random.shuffle(peer_list)

            if not peer_list:
                print("[ERROR] No peers received from any tracker. Exiting.")
                return

            print(f"[TRACKER] Total peers received: {len(peer_list)}")
            print(f"[DEBUG] First 10 peers: {peer_list[:10]}")

            self._start_listener()

            # connect to peers
            print("\n[PEERS] Connecting to peers (capped)...")
            for ip, port in peer_list[:50]:
                if self._stop or len(self.peers) >= self.max_peers:
                    break
                try:
                    self._connect_to_peer(ip, port)
                except Exception as e:
                    print(f"[PEERS] Connect error to {ip}:{port} - {e}")

            print(f"[PEERS] Connected to {len(self.peers)} peers")
            if not self.peers:
                print("[ERROR] Could not connect to any peers. Exiting.")
                return

            # start peer worker threads
            print("\n[DOWNLOAD] Starting peer worker threads...")
            for peer in list(self.peers):
                t = threading.Thread(
                    target=self._peer_worker,
                    args=(peer,),
                    daemon=True
                )
                t.start()
                self._peer_threads.append(t)

            # schedule periodic tracker reannounces
            self._schedule_reannounce()

            # start progress monitor
            self._monitor_progress()

            # main wait loop
            while not self._stop and not self.piece_manager.is_complete():
                time.sleep(0.5)

            # completed
            if self.piece_manager.is_complete():
                print("\n[SUCCESS] Download complete!")
                self._announce_completed()

        except KeyboardInterrupt:
            print("\n[STOP] Interrupted by user")
            self._stop = True

        except Exception as e:
            print(f"\n[ERROR] Fatal error: {e}")
            import traceback
            traceback.print_exc()
            self._stop = True

        finally:
            print("\n[CLEANUP] Shutting down client...")
            self._cleanup()


    # tracker reannounce
    def _schedule_reannounce(self):
        def reannounce():
            if self._stop:
                return
            print("[TRACKER] Re-announcing to trackers...")
            left = max(0, self.torrent.length - self.downloaded)

            peer_list = []
            for tracker_url in self.torrent.announce_list:
                self.tracker.announce_url = tracker_url
                try:
                    peers = self.tracker.announce(downloaded=self.downloaded, left=left, uploaded=self.uploaded)
                    peer_list.extend(peers)
                except Exception:
                    continue

            # remove duplicates
            peer_list = list(set(peer_list))
            random.shuffle(peer_list)

            for ip, port in peer_list:
                if self._stop or len(self.peers) >= self.max_peers:
                    break
                try:
                    self._connect_to_peer(ip, port)
                except Exception:
                    pass

            if not self._stop:
                interval = self.tracker.get_interval() or 120
                self._reannounce_timer = threading.Timer(interval, reannounce)
                self._reannounce_timer.daemon = True
                self._reannounce_timer.start()

        interval = self.tracker.get_interval() or 120
        self._reannounce_timer = threading.Timer(interval, reannounce)
        self._reannounce_timer.daemon = True
        self._reannounce_timer.start()


    # accept inbound peers
    def _start_listener(self):
        import socket

        def accept_loop():
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("0.0.0.0", self.listen_port))
            server.listen(50)
            print(f"[LISTEN] Accepting inbound peers on port {self.listen_port}...")

            while not self._stop:
                try:
                    conn, addr = server.accept()
                    ip, port = addr
                    print(f"[INBOUND] Peer connected from {ip}:{port}")
                    peer = PeerConnection.from_socket(
                        conn, ip, port,
                        self.torrent.info_hash,
                        self.peer_id
                    )
                    with self._lock:
                        self.peers.append(peer)
                    t = threading.Thread(target=self._peer_worker, args=(peer,), daemon=True)
                    t.start()
                    self._peer_threads.append(t)
                except Exception:
                    continue

            server.close()

        threading.Thread(target=accept_loop, daemon=True).start()

    # outbound peer connect
    def _connect_to_peer(self, ip: str, port: int) -> bool:
        print(f"[CONNECT] Trying {ip}:{port}")  # debug
        try:
            pc = PeerConnection(ip, port, self.torrent.info_hash, self.peer_id)
            if pc.connect(timeout=self.connect_timeout):
                print(f"[CONNECT] TCP connected {ip}:{port}")  # debug
                try:
                    bitfield = self.piece_manager.get_bitfield()
                    pc.send_bitfield(bitfield)
                except Exception as e:
                    print(f"[CONNECT] Failed to send bitfield to {ip}:{port} -> {e}")

                pc.send_interested()
                with self._lock:
                    self.peers.append(pc)

                print(f"[+] Handshake OK {ip}:{port}")  # debug
                return True
            else:
                print(f"[-] TCP failed {ip}:{port}")
                return False

        except Exception as e:
            print(f"[CONNECT-FAIL] {ip}:{port} -> {e}")
            return False

    # peer worker
    def _peer_worker(self, peer: PeerConnection):
        peer_id = f"{peer.ip}:{peer.port}"
        print(f"[WORKER] Started thread for {peer_id}")

        request_queue = []
        BLOCK_SIZE = 16384
        MAX_PENDING = 32
        last_keepalive = time.time()
        KEEPALIVE_INTERVAL = 120

        try:
            while (
                not self._stop
                and not self.piece_manager.is_complete()
                and peer.connected
            ):
                try:
                    msg = peer.receive_message(timeout=0.2)
                    if msg is not None:
                        # prevent printing full blocks
                        if msg.get("type") == "piece":
                            print(f"[{peer_id}] <-- PIECE index={msg['index']} begin={msg['begin']} size={len(msg['block'])} bytes")
                        else:
                            if msg is not None:
                                if msg.get("type") == "piece":
                                    print(f"[{peer_id}] <-- PIECE index={msg['index']} begin={msg['begin']} size={len(msg['block'])}")
                                else:
                                    print(f"[{peer_id}] <-- {msg}")
                except Exception as e:
                    print(f"[{peer_id}] Error receiving message: {e}")
                    break

                if msg is not None:
                    mtype = msg.get("type")
                    if mtype == "choke":
                        peer.peer_choking = True
                        request_queue.clear()

                    elif mtype == "unchoke":
                        peer.peer_choking = False
                        peer.send_interested()

                    elif mtype == "interested":
                        peer.peer_interested = True

                    elif mtype == "not_interested":
                        peer.peer_interested = False

                    elif mtype == "have":
                        piece_index = msg["piece_index"]
                        if not peer.bitfield:
                            peer.bitfield = [False] * self.torrent.num_pieces()
                        elif piece_index >= len(peer.bitfield):
                            extra = piece_index + 1 - len(peer.bitfield)
                            peer.bitfield.extend([False] * extra)
                        already_had = peer.bitfield[piece_index]
                        peer.bitfield[piece_index] = True
                        # count availability for rarest-first
                        self.piece_manager.update_have(piece_index, already_had)

                    elif mtype == "bitfield":
                        old = list(peer.bitfield) if peer.bitfield else []
                        peer.bitfield = msg["bitfield"]
                        # update availability counts
                        self.piece_manager.update_peer_bitfield(old, peer.bitfield)
                        print(
                            f"[{peer_id}] Received bitfield: "
                            f"{sum(1 for b in peer.bitfield if b)}/{len(peer.bitfield)} pieces"
                        )

                    elif mtype == "piece":
                        try:
                            piece_index = msg["index"]
                            begin = msg["begin"]
                            block = msg["block"]
                            print(f"[{peer_id}] blk {begin//16384+1}/16 received for piece {piece_index}")

                            complete = self.piece_manager.add_block(piece_index, begin, block)
                            if (piece_index, begin, len(block)) in request_queue:
                                request_queue.remove((piece_index, begin, len(block)))

                            if complete:
                                request_queue = [r for r in request_queue if r[0] != piece_index]
                                print(f"[{peer_id}] PIECE {piece_index} COMPLETED — verifying hash...")

                                piece_data = self.piece_manager.get_piece_data(piece_index)
                                if self._verify_piece(piece_index, piece_data):
                                    print(f"[{peer_id}] VERIFIED ✓ writing to disk")
                                    self._write_piece(piece_index, piece_data)
                                    self._broadcast_have(piece_index)
                                else:
                                    print(f"[{peer_id}] HASH FAIL ✗ resetting piece")
                                    self.piece_manager.reset_piece(piece_index)

                        except Exception as e:
                            print(f"[{peer_id}] PIECE HANDLING ERROR: {e}")
                            import traceback; traceback.print_exc()
                            continue

                    elif mtype == "request":
                        self._handle_request(peer, msg["index"], msg["begin"], msg["length"])

                    elif mtype == "cancel":
                        pass

                # request next blocks
                if not peer.peer_choking:
                    # pick a piece to download for this peer
                    piece_index = self._select_piece(peer)

                    # continue requesting blocks on already-active piece as well
                    if piece_index is None and request_queue:
                        piece_index = request_queue[0][0]

                    if piece_index is not None:
                        total_len = self._get_piece_length(piece_index)
                        offset = 0

                        # request many blocks in parallel
                        while offset < total_len and len(request_queue) < MAX_PENDING:
                            if self.piece_manager.has_block(piece_index, offset):
                                offset += BLOCK_SIZE
                                continue

                            if self.piece_manager.is_block_pending(piece_index, offset):
                                if not self.piece_manager.is_block_stale(piece_index, offset):
                                    offset += BLOCK_SIZE
                                    continue
                                self.piece_manager.clear_block_pending(piece_index, offset)

                            block_len = min(BLOCK_SIZE, total_len - offset)
                            try:
                                peer.send_request(piece_index, offset, block_len)
                                self.piece_manager.mark_block_requested(piece_index, offset, block_len)
                                request_queue.append((piece_index, offset, block_len))
                                print(f"[{peer_id}] REQUEST piece={piece_index} offset={offset} len={block_len}")
                            except:
                                self.piece_manager.reset_piece(piece_index)
                                break

                            offset += BLOCK_SIZE


                now = time.time()
                if now - last_keepalive > KEEPALIVE_INTERVAL:
                    try:
                        peer.send_keepalive()
                    except Exception:
                        pass
                    last_keepalive = now

                # free stale pending blocks
                self.piece_manager.reclaim_stale_blocks()

                time.sleep(0.01)

        except Exception as e:
            if not self._stop:
                print(f"[{peer_id}] Worker thread error: {e}")

        finally:
            print(f"[WORKER] Stopped thread for {peer_id}")
            try:
                peer.close()
            except Exception:
                pass
            with self._lock:
                try:
                    self.peers.remove(peer)
                except ValueError:
                    pass

    # piece selection
    def _select_piece(self, peer: PeerConnection) -> Optional[int]:
        candidates = []
        for i in range(self.torrent.num_pieces()):
            if (
                not self.piece_manager.is_piece_complete(i)
                and not self.piece_manager.is_piece_in_progress(i)
                and peer.has_piece(i)
            ):
                avail = self.piece_manager.get_availability(i)
                candidates.append((avail, random.random(), i))

        if not candidates:
            return None

        _, _, idx = min(candidates, key=lambda x: (x[0], x[1]))
        self.piece_manager.mark_piece_in_progress(idx)
        return idx



    # piece length
    def _get_piece_length(self, piece_index: int) -> int:
        if piece_index == self.torrent.num_pieces() - 1:
            return self.torrent.length - (piece_index * self.torrent.piece_length)
        else:
            return self.torrent.piece_length

    # verify piece hash
    def _verify_piece(self, piece_index: int, data: bytes) -> bool:
        expected_hash = self.torrent.pieces[piece_index]
        actual_hash = hashlib.sha1(data).digest()
        return expected_hash == actual_hash

    # write verified piece
    def _write_piece(self, piece_index: int, data: bytes):
        try:
            self.disk.write_piece(piece_index, data, self.torrent.piece_length)
            self.piece_manager.mark_piece_complete(piece_index)
            with self._lock:
                self.downloaded += len(data)
        except Exception as e:
            print(f"[DISK] Error writing piece {piece_index}: {e}")
            self.piece_manager.reset_piece(piece_index)

    # send have to peers
    def _broadcast_have(self, piece_index: int):
        with self._lock:
            for peer in list(self.peers):
                try:
                    peer.send_have(piece_index)
                except Exception:
                    pass

    # serve incoming request
    def _handle_request(self, peer: PeerConnection, index: int, begin: int, length: int):
        try:
            if not self.piece_manager.is_piece_complete(index):
                return
            piece_data = self.disk.read_piece(index, self.torrent.piece_length)
            block = piece_data[begin : begin + length]
            peer.send_piece(index, begin, block)
            with self._lock:
                self.uploaded += len(block)
        except Exception as e:
            print(f"[UPLOAD] Error handling request: {e}")

    # progress monitor
    def _monitor_progress(self):
        def monitor():
            last_downloaded = 0
            while not self._stop and not self.piece_manager.is_complete():
                time.sleep(5)
                with self._lock:
                    current = self.downloaded
                    speed = (current - last_downloaded) / 5.0
                    last_downloaded = current
                    percent = (current / self.torrent.length) * 100 if self.torrent.length else 0
                    peers_count = len([p for p in self.peers if getattr(p, "connected", False)])
                print(
                    f"[PROGRESS] {percent:.3f}% | {current}/{self.torrent.length} bytes | "
                    f"Speed: {speed/1024:.1f} KB/s | Peers: {peers_count}"
                )

        self._monitor_thread = threading.Thread(target=monitor, daemon=True)
        self._monitor_thread.start()


    def _announce_completed(self):
        try:
            print("[TRACKER] Announcing completion to tracker...")
            self.tracker.announce(
                downloaded=self.downloaded,
                left=0,
                uploaded=self.uploaded,
                event="completed",
            )
        except Exception as e:
            print(f"[TRACKER] Completion announce failed: {e}")

    # cleanup
    def _cleanup(self):
        self._stop = True
        if self._reannounce_timer:
            try:
                self._reannounce_timer.cancel()
            except Exception:
                pass

        with self._lock:
            for peer in list(self.peers):
                try:
                    peer.close()
                except Exception:
                    pass
            self.peers.clear()

        for thread in self._peer_threads:
            thread.join(timeout=1.0)

        if self._monitor_thread:
            self._monitor_thread.join(timeout=1.0)

        try:
            self.disk.finalize()
        except Exception:
            pass

        if self.start_time:
            elapsed = time.time() - self.start_time
            print(f"\n[STATS] Downloaded: {self.downloaded} bytes")
            print(f"[STATS] Uploaded: {self.uploaded} bytes")
            print(f"[STATS] Time: {elapsed:.1f} seconds")
            if elapsed > 0:
                print(f"[STATS] Average speed: {self.downloaded/elapsed/1024:.1f} KB/s")

    # stop client
    def stop(self):
        print("\n[STOP] Stopping client...")
        self._stop = True
