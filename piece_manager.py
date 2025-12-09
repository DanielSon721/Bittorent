import hashlib
import time
import logging
from typing import List, Optional
from disk_io import DiskIO

BLOCK_SIZE = 16384  # 16kb block
logger = logging.getLogger("bittorrent")

class PieceManager:
    # tracks piece status and data
    def __init__(self, torrent_meta):
        self.torrent = torrent_meta  # torrent meta
        self.total_pieces = torrent_meta.num_pieces()  # piece count
        self.pieces_state = ["missing"] * self.total_pieces  # missing/requested/complete

        self.writer = DiskIO(torrent_meta.name, torrent_meta.length)

        self.buffers = [dict() for _ in range(self.total_pieces)]  # not used
        self.pieces_data = [None] * self.total_pieces
        self.blocks_received = [set() for _ in range(self.total_pieces)]  # received ranges

        # block tracking
        self.block_states = [dict() for _ in range(self.total_pieces)]  # offset -> meta

        # availability counts (rarest-first)
        self.availability = [0] * self.total_pieces

    def next_piece_for_peer(self, peer_bitfield: bytes) -> Optional[int]:
        binary_bits = ''.join(f"{byte:08b}" for byte in peer_bitfield)
        for i in range(self.total_pieces):
            if self.pieces_state[i] == "missing" and binary_bits[i] == '1':
                self.pieces_state[i] = "requested"
                return i
        return None

    def piece_length(self, index: int) -> int:
        if index == self.total_pieces - 1:
            return self.torrent.length - (index * self.torrent.piece_length)
        return self.torrent.piece_length

    def verify_piece(self, index: int, data: bytes) -> bool:
        expected_hash = self.torrent.pieces[index]
        actual_hash = hashlib.sha1(data).digest()
        return expected_hash == actual_hash

    def mark_piece_complete(self, index: int):
        self.pieces_state[index] = "complete"

    def has_any_pieces(self) -> bool:
        return "complete" in self.pieces_state

    def get_bitfield(self):
        return [state == "complete" for state in self.pieces_state]

    def is_piece_complete(self, index: int) -> bool:
        return self.pieces_state[index] == "complete"

    def is_piece_in_progress(self, index: int) -> bool:
        return self.pieces_state[index] == "requested"

    def mark_piece_in_progress(self, index: int):
        if self.pieces_state[index] == "missing":
            self.pieces_state[index] = "requested"

    # availability helpers
    def update_peer_bitfield(self, old: List[bool], new: List[bool]):
        if len(new) < self.total_pieces:
            new = new + [False] * (self.total_pieces - len(new))
        if len(old) < self.total_pieces:
            old = old + [False] * (self.total_pieces - len(old))
        for i in range(self.total_pieces):
            if not old[i] and new[i]:
                self.availability[i] += 1
            elif old[i] and not new[i]:
                self.availability[i] = max(0, self.availability[i] - 1)

    def update_have(self, piece_index: int, already_had: bool):
        if piece_index >= self.total_pieces:
            return
        if not already_had:
            self.availability[piece_index] += 1

    def get_availability(self, piece_index: int) -> int:
        if piece_index >= self.total_pieces:
            return 0
        return self.availability[piece_index]

    def has_block(self, index: int, offset: int) -> bool:
        for start, end in self.blocks_received[index]:
            if start <= offset < end:
                return True
        return False

    # block helpers
    def mark_block_requested(self, index: int, offset: int, length: int):
        self.block_states[index][offset] = {"state": "pending", "ts": time.time(), "length": length}

    def mark_block_received(self, index: int, offset: int, length: int):
        self.block_states[index][offset] = {"state": "full", "ts": time.time(), "length": length}

    def is_block_pending(self, index: int, offset: int) -> bool:
        meta = self.block_states[index].get(offset)
        return bool(meta) and meta.get("state") == "pending"

    def is_block_stale(self, index: int, offset: int, max_age: float = 15.0) -> bool:
        meta = self.block_states[index].get(offset)
        if not meta or meta.get("state") != "pending":
            return False
        return (time.time() - meta.get("ts", 0.0)) > max_age

    def clear_block_pending(self, index: int, offset: int):
        if offset in self.block_states[index]:
            self.block_states[index].pop(offset, None)

    def reclaim_stale_blocks(self, max_age: float = 10.0):
        now = time.time()
        for idx in range(self.total_pieces):
            if self.pieces_state[idx] != "requested":
                continue
            stale_offsets = [
                off for off, meta in self.block_states[idx].items()
                if meta.get("state") == "pending" and (now - meta.get("ts", 0.0)) > max_age
            ]
            for off in stale_offsets:
                self.block_states[idx].pop(off, None)
            # if nothing is pending anymore, allow reselection
            if not self.block_states[idx] and not self.is_piece_complete(idx):
                logger.debug(f"resetting piece {idx} after stale requests")
                self.pieces_state[idx] = "missing"

    def has_pending_blocks(self) -> bool:
        """Return True if any block is still marked pending."""
        for states in self.block_states:
            for meta in states.values():
                if meta.get("state") == "pending":
                    return True
        return False

    def reset_in_progress(self):
        """reset requested pieces so they can be retried"""
        for idx, state in enumerate(self.pieces_state):
            if state == "requested":
                self.pieces_state[idx] = "missing"
                self.block_states[idx].clear()

    def add_block(self, index, begin, block):
        piece_len = self.piece_length(index)

        if self.pieces_data[index] is None:
            self.pieces_data[index] = bytearray(piece_len)

        end = begin + len(block)
        self.pieces_data[index][begin:end] = block
        self.blocks_received[index].add((begin, end))
        self.mark_block_received(index, begin, len(block))

        merged = sorted(self.blocks_received[index])
        current_end = 0
        for s, e in merged:
            if s > current_end:
                return False
            current_end = max(current_end, e)

        return current_end >= piece_len

    def get_piece_data(self, index: int) -> bytes:
        data = self.pieces_data[index]
        return bytes(data) if data else b""

    def reset_piece(self, index: int):
        self.pieces_state[index] = "missing"
        self.buffers[index].clear()
        self.pieces_data[index] = None
        self.blocks_received[index].clear()
        self.block_states[index].clear()

    def num_completed(self) -> int:
        return self.pieces_state.count("complete")

    def is_complete(self) -> bool:
        return self.num_completed() == self.total_pieces
    
    def is_piece_in_progress(self, index: int) -> bool:
        return self.pieces_state[index] == "requested"

    def mark_piece_in_progress(self, index: int):
        if self.pieces_state[index] == "missing":
            self.pieces_state[index] = "requested"
