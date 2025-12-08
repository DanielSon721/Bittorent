# piece_manager.py
import hashlib
from typing import List, Optional
from disk_io import DiskIO

class PieceManager:
    def __init__(self, torrent_meta):
        self.torrent = torrent_meta  # Torrent metadata
        self.total_pieces = torrent_meta.num_pieces()  # Number of pieces
        self.pieces_state = ["missing"] * self.total_pieces  # missing/requested/complete

        self.writer = DiskIO(torrent_meta.name, torrent_meta.length)

        self.buffers = [dict() for _ in range(self.total_pieces)]
        self.pieces_data = [None] * self.total_pieces
        self.blocks_received = [set() for _ in range(self.total_pieces)]

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

    def has_block(self, index: int, offset: int) -> bool:
        for start, end in self.blocks_received[index]:
            if start <= offset < end:
                return True
        return False

    def add_block(self, index, begin, block):
        piece_len = self.piece_length(index)

        if self.pieces_data[index] is None:
            self.pieces_data[index] = bytearray(piece_len)

        end = begin + len(block)
        self.pieces_data[index][begin:end] = block
        self.blocks_received[index].add((begin, end))

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

    def num_completed(self) -> int:
        return self.pieces_state.count("complete")

    def is_complete(self) -> bool:
        return self.num_completed() == self.total_pieces
    
    def is_piece_in_progress(self, index: int) -> bool:
        return self.pieces_state[index] == "requested"

    def mark_piece_in_progress(self, index: int):
        if self.pieces_state[index] == "missing":
            self.pieces_state[index] = "requested"

