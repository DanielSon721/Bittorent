import os
import threading
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
from tracker import TrackerClient
from peer_connection import PeerConnection
from piece_manager import PieceManager
from disk_io import DiskIO
import hashlib

class BitTorrentClient:
    def __init__(self, torrent, peer_id: bytes, listen_port: int, output_path: Optional[str] = None, max_peers: int = 100, seed_mode: bool = False, extra_peers: Optional[list] = None):
        self.torrent = torrent
        self.peer_id = peer_id  # peer id
        self.listen_port = listen_port  # listen port
        self.output_path = output_path or torrent.name  # output path
        self.seed_mode = seed_mode  # seeding flag
        self.extra_peers = extra_peers or []  # manually supplied peers
        self.logger = logging.getLogger("bittorrent")
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

        # tracker client
        self.tracker = TrackerClient(
            announce_url=torrent.announce,
            info_hash=torrent.info_hash,
            peer_id=peer_id,
            port=listen_port,
        )

        self.piece_manager = PieceManager(torrent)  # piece state
        self.disk = DiskIO(
            self.output_path,
            torrent.length,
            preallocate=not seed_mode,
            truncate=not seed_mode,
        )  # disk writer
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
        self._upload_notice_shown = False
        self._last_upload_print = 0.0

        # limits
        self.max_peers = max_peers
        self.connect_timeout = 5.0
        self._flush_requests = False

    # main entry
    def run(self):
        print(f"Starting BitTorrent client for: {self.torrent.name}")
        print(f"Total size: {self.torrent.length} bytes ({self.torrent.num_pieces()} pieces)")
        self.start_time = time.time()

        if self.seed_mode:
            try:
                self._initialize_seeding_state()
            except Exception as e:
                print(f"Seeding initialization failed: {e}")
                return

        try:
            # announce to trackers
            print("\nAnnouncing to trackers...")
            left = 0 if self.seed_mode else max(0, self.torrent.length - self.downloaded)
            peer_list = []

            for tracker_url in self.torrent.announce_list:
                self.tracker.announce_url = tracker_url
                try:
                    peers = self.tracker.announce(downloaded=self.downloaded, left=left, uploaded=self.uploaded, event="started")
                    peer_list.extend(peers)
                except Exception as e:
                    print(f"Failed to announce to {tracker_url}: {e}")

            # append any manual peers (ip:port strings)
            for entry in self.extra_peers:
                try:
                    ip, port = entry.split(":")
                    peer_list.append((ip, int(port)))
                except Exception:
                    print(f"Invalid --peer entry ignored: {entry}")
            
            # remove duplicates
            peer_list = list(set(peer_list))
            random.shuffle(peer_list)

            if not peer_list:
                print("ERROR | No peers received from any tracker. Exiting.")
                return

            print(f"Total peers received: {len(peer_list)}")

            self._start_listener()

            # connect to peers
            print("\nConnecting to peers...")
            self._connect_to_peers_parallel(peer_list[:50])

            print(f"Connected to {len(self.peers)} peers")
            if not self.peers:
                print("ERROR | Could not connect to any peers. Exiting.")
                return

            # start peer worker threads
            print("\nStarting peer worker threads...")
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
            while not self._stop:
                if (not self.seed_mode) and self.piece_manager.is_complete():
                    break
                time.sleep(0.5)

            # completed
            if (not self.seed_mode) and self.piece_manager.is_complete():
                # final progress line at 100%
                self.downloaded = self._calculate_downloaded_bytes()
                peers_count = len([p for p in self.peers if getattr(p, "connected", False)])
                self._print_progress(self.downloaded, 0.0, peers_count, final=True)
                time.sleep(1)
                print("\nDownload complete!")
                self._announce_completed()

        except KeyboardInterrupt:
            print("\nInterrupted by user")
            self._stop = True

        except Exception as e:
            print(f"\nFatal error: {e}")
            import traceback
            traceback.print_exc()
            self._stop = True

        finally:
            print("\nShutting down client...")
            self._cleanup()


    def _initialize_seeding_state(self):
        print("Validating existing data for seeding...")
        if not os.path.exists(self.output_path):
            raise FileNotFoundError(f"Payload not found at {self.output_path}")

        actual_size = os.path.getsize(self.output_path)
        if actual_size < self.torrent.length:
            raise ValueError(f"Payload size ({actual_size}) smaller than torrent length ({self.torrent.length})")
        if actual_size > self.torrent.length:
            self.logger.warning("Payload size larger than torrent length; extra bytes will be ignored")

        for idx in range(self.torrent.num_pieces()):
            expected_len = self._get_piece_length(idx)
            data = self.disk.read_piece(idx, expected_len)
            if len(data) < expected_len:
                raise ValueError(f"Piece {idx} shorter than expected length {expected_len}")
            if not self._verify_piece(idx, data):
                raise ValueError(f"Hash mismatch for piece {idx}; cannot seed with corrupt data")

        self.piece_manager.mark_all_complete()
        self.downloaded = self.torrent.length
        print("All pieces verified. Ready to seed.")


    # tracker reannounce
    def _schedule_reannounce(self):
        def reannounce():
            if self._stop:
                return
            print("Re-announcing to trackers...")
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

            self._connect_to_peers_parallel(peer_list)

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
            print(f"Accepting inbound peers on port {self.listen_port}...")

            while not self._stop:
                try:
                    conn, addr = server.accept()
                    ip, port = addr
                    print(f"Peer connected from {ip}:{port}")
                    peer = PeerConnection.from_socket(
                        conn, ip, port,
                        self.torrent.info_hash,
                        self.peer_id
                    )
                    try:
                        bitfield = self.piece_manager.get_bitfield()
                        peer.send_bitfield(bitfield)
                        if self.seed_mode or self.piece_manager.has_any_pieces():
                            peer.send_unchoke()
                            peer.am_choking = False
                    except Exception:
                        pass
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
        with self._lock:
            if self._stop or len(self.peers) >= self.max_peers:
                return False
        try:
            pc = PeerConnection(ip, port, self.torrent.info_hash, self.peer_id)
            if pc.connect(timeout=self.connect_timeout):
                try:
                    bitfield = self.piece_manager.get_bitfield()
                    pc.send_bitfield(bitfield)
                except Exception:
                    pass
                if self.seed_mode or self.piece_manager.has_any_pieces():
                    try:
                        pc.send_unchoke()
                        pc.am_choking = False
                    except Exception:
                        pass

                if not self.seed_mode:
                    pc.send_interested()
                with self._lock:
                    if self._stop or len(self.peers) >= self.max_peers:
                        pc.close()
                        return False
                    self.peers.append(pc)

                print(f"{ip}:{port} connected")
                return True
            else:
                return False

        except Exception:
            return False

    # peer worker
    def _peer_worker(self, peer: PeerConnection):
        peer_id = f"{peer.ip}:{peer.port}"
        self.logger.debug(f"Started thread for {peer_id}")

        request_queue = []
        BLOCK_SIZE = 16384
        MAX_PENDING = 64
        last_keepalive = time.time()
        KEEPALIVE_INTERVAL = 120
        last_data = time.time()
        STALL_TIMEOUT = 8

        try:
            while (
                not self._stop
                and (self.seed_mode or not self.piece_manager.is_complete())
                and peer.connected
            ):
                if self._flush_requests:
                    for pi, off, ln in list(request_queue):
                        self.piece_manager.clear_block_pending(pi, off)
                        request_queue.remove((pi, off, ln))
                    self._flush_requests = False
                try:
                    msg = peer.receive_message(timeout=0.2)
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
                        if self.seed_mode or self.piece_manager.has_any_pieces():
                            self._unchoke_peer(peer)

                    elif mtype == "not_interested":
                        peer.peer_interested = False
                        self._choke_peer(peer)

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

                    elif mtype == "piece":
                        try:
                            piece_index = msg["index"]
                            begin = msg["begin"]
                            block = msg["block"]
                            complete = self.piece_manager.add_block(piece_index, begin, block)
                            last_data = time.time()
                            if (piece_index, begin, len(block)) in request_queue:
                                request_queue.remove((piece_index, begin, len(block)))

                            if complete:
                                request_queue = [r for r in request_queue if r[0] != piece_index]
                                piece_data = self.piece_manager.get_piece_data(piece_index)
                                if self._verify_piece(piece_index, piece_data):
                                    self._write_piece(piece_index, piece_data)
                                    self._broadcast_have(piece_index)
                                else:
                                    print(f"[{peer_id}] hash failed, resetting piece {piece_index}")
                                    self.piece_manager.reset_piece(piece_index)

                        except Exception as e:
                            print(f"{peer_id} | PIECE HANDLING ERROR: {e}")
                            import traceback; traceback.print_exc()
                            continue

                    elif mtype == "request":
                        self._handle_request(peer, msg["index"], msg["begin"], msg["length"])

                    elif mtype == "cancel":
                        pass

                # request next blocks
                if not peer.peer_choking and not self.seed_mode:
                    # drop stale requests so queue can move
                    for pi, off, ln in list(request_queue):
                        if self.piece_manager.is_block_stale(pi, off):
                            self.logger.debug(f"{peer_id} cleared stale request piece={pi} off={off}")
                            request_queue.remove((pi, off, ln))
                            self.piece_manager.clear_block_pending(pi, off)

                    # pick a piece to download for this peer
                    piece_index = self._select_piece(peer)
                    if piece_index is None:
                        self.logger.debug(f"{peer_id} waiting for available piece (queue={len(request_queue)})")

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
                                self.logger.debug(f"{peer_id} requesting piece={piece_index} off={offset} len={block_len} q={len(request_queue)}")
                            except:
                                self.piece_manager.reset_piece(piece_index)
                                break

                            offset += BLOCK_SIZE


                now = time.time()
                # if queued but no data for a while, drop requests to unblock
                if not self.seed_mode and request_queue and (now - last_data) > STALL_TIMEOUT:
                    stalled_pieces = {pi for pi, _, _ in request_queue}
                    for pi, off, ln in list(request_queue):
                        self.piece_manager.clear_block_pending(pi, off)
                        request_queue.remove((pi, off, ln))
                    for pi in stalled_pieces:
                        # allow other peers to pick up the piece without discarding data
                        self.piece_manager.allow_piece_retry(pi)
                    self.logger.debug(f"{peer_id} stalled {STALL_TIMEOUT}s, cleared queue")

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
                self.logger.debug(f"[{peer_id}] Worker thread error: {e}")

        finally:
            self.logger.debug(f"Stopped thread for {peer_id}")
            for pi, off, ln in list(request_queue):
                self.piece_manager.clear_block_pending(pi, off)
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

    def _calculate_downloaded_bytes(self) -> int:
        total = 0
        for idx in range(self.torrent.num_pieces()):
            if self.piece_manager.is_piece_complete(idx):
                total += self._get_piece_length(idx)
        return total

    def _print_progress(self, current: int, speed_bytes_per_s: float, peers_count: int, final: bool = False):
        percent = (current / self.torrent.length) * 100 if self.torrent.length else 0
        if final:
            percent = 100.0
            current = self.torrent.length
            speed_bytes_per_s = 0.0
        print(
            "------------------------------------------------------------------------------------\n"
            f"PROGRESS | {percent:.3f}% | {current}/{self.torrent.length} bytes | "
            f"Speed: {speed_bytes_per_s/1024:.1f} KB/s | Peers: {peers_count}\n"
            "------------------------------------------------------------------------------------"
        )

    # verify piece hash
    def _verify_piece(self, piece_index: int, data: bytes) -> bool:
        expected_hash = self.torrent.pieces[piece_index]
        actual_hash = hashlib.sha1(data).digest()
        return expected_hash == actual_hash

    # write verified piece
    def _write_piece(self, piece_index: int, data: bytes):
        if self.piece_manager.is_piece_complete(piece_index):
            return
        try:
            self.disk.write_piece(piece_index, data, self.torrent.piece_length)
            self.piece_manager.mark_piece_complete(piece_index)
            with self._lock:
                self.downloaded = self._calculate_downloaded_bytes()
        except Exception as e:
            print(f"Error writing piece {piece_index}: {e}")
            self.piece_manager.reset_piece(piece_index)

    # send have to peers
    def _broadcast_have(self, piece_index: int):
        with self._lock:
            for peer in list(self.peers):
                try:
                    peer.send_have(piece_index)
                except Exception:
                    pass

    def _unchoke_peer(self, peer: PeerConnection):
        try:
            peer.send_unchoke()
            peer.am_choking = False
        except Exception:
            pass

    def _choke_peer(self, peer: PeerConnection):
        try:
            peer.send_choke()
            peer.am_choking = True
        except Exception:
            pass

    # serve incoming request
    def _handle_request(self, peer: PeerConnection, index: int, begin: int, length: int):
        try:
            if not self.piece_manager.is_piece_complete(index):
                return
            piece_len = self._get_piece_length(index)
            if begin < 0 or length <= 0 or begin + length > piece_len:
                return
            peer_id = f"{peer.ip}:{peer.port}"
            piece_data = self.disk.read_piece(index, piece_len)
            block = piece_data[begin : begin + length]
            peer.send_piece(index, begin, block)
            with self._lock:
                self.uploaded += len(block)
                if not self.logger.isEnabledFor(logging.DEBUG):
                    now = time.time()
                    if (not self._upload_notice_shown) or (now - self._last_upload_print >= 5):
                        print(f"Uploading to peers... uploaded {self.uploaded} bytes")
                        self._upload_notice_shown = True
                        self._last_upload_print = now
            self.logger.debug(f"UPLOAD | {peer_id} piece={index} off={begin} len={len(block)} uploaded={self.uploaded}")
        except Exception as e:
            self.logger.debug(f"Upload handling error: {e}")

    # progress monitor
    def _monitor_progress(self):
        def monitor():
            last_downloaded = 0
            stall_ticks = 0
            while not self._stop and not self.piece_manager.is_complete():
                time.sleep(5)
                with self._lock:
                    # recalc in case counts drift
                    self.downloaded = self._calculate_downloaded_bytes()
                    current = self.downloaded
                    speed = (current - last_downloaded) / 5.0
                    last_downloaded = current
                    percent = (current / self.torrent.length) * 100 if self.torrent.length else 0
                    peers_count = len([p for p in self.peers if getattr(p, "connected", False)])
                    if speed == 0:
                        stall_ticks += 1
                    else:
                        stall_ticks = 0
                    # Nudge stalled downloads sooner by clearing pending requests
                    if stall_ticks >= 2:
                        self.piece_manager.reclaim_stale_blocks(max_age=5.0)
                        self.logger.debug("stalled; resetting in-progress pieces and flushing requests")
                        self.piece_manager.reset_in_progress()
                        self._flush_requests = True
                        stall_ticks = 0
                    if self._stop or self.piece_manager.is_complete():
                        break
                self._print_progress(current, speed, peers_count)

        self._monitor_thread = threading.Thread(target=monitor, daemon=True)
        self._monitor_thread.start()

    def _connect_to_peers_parallel(self, peer_list, max_workers: int = 20):
        if not peer_list:
            return
        worker_count = max(1, min(max_workers, len(peer_list)))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = []
            for ip, port in peer_list:
                with self._lock:
                    if self._stop or len(self.peers) >= self.max_peers:
                        break
                futures.append(executor.submit(self._connect_to_peer, ip, port))

            for future in futures:
                try:
                    future.result()
                except Exception:
                    continue


    def _announce_completed(self):
        try:
            print("Announcing completion to tracker...")
            self.tracker.announce(
                downloaded=self.downloaded,
                left=0,
                uploaded=self.uploaded,
                event="completed",
            )
        except Exception as e:
            print(f"Completion announce failed: {e}")

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

    # stop client
    def stop(self):
        self._stop = True
