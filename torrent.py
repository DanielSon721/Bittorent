import hashlib
import bencodepy
from typing import List, Tuple, Dict, Union
import os

class TorrentFile:
    def __init__(self, path: List[str], length: int):
        self.path = path
        self.length = length

    def joined_path(self) -> str:
        return os.path.join(*self.path)

class TorrentMeta:
    # holds parsed torrent info
    def __init__(self, raw, info_hash: bytes, piece_length: int, pieces: List[bytes],
                 announce: str, name: str, length: int,
                 files: Union[None, List['TorrentFile']] = None,
                 announce_list: List[str] = None):
        self.raw = raw
        self.info_hash = info_hash
        self.piece_length = piece_length
        self.pieces = pieces
        self.announce = announce
        self.announce_list = announce_list or [announce]   # tracker list
        self.name = name
        self.length = length
        self.files = files

    @classmethod
    def from_file(cls, path: str) -> "TorrentMeta":
        with open(path, "rb") as f:
            raw = bencodepy.decode(f.read())

        info = raw[b"info"]
        encoded_info = bencodepy.encode(info)
        info_hash = hashlib.sha1(encoded_info).digest()

        piece_length = info[b"piece length"]
        pieces_blob = info[b"pieces"]
        pieces = [pieces_blob[i:i+20] for i in range(0, len(pieces_blob), 20)]

        announce = raw[b"announce"].decode()

        # collect announce list
        announce_list = [announce]
        if b"announce-list" in raw:
            for tier in raw[b"announce-list"]:
                for url in tier:
                    u = url.decode()
                    if u not in announce_list:
                        announce_list.append(u)

        name = info[b"name"].decode()

        # handle single or multi file
        if b"length" in info:
            length = info[b"length"]
            files = None
        elif b"files" in info:
            files = []
            total = 0
            for f in info[b"files"]:
                fn_len = f[b"length"]
                path_components = [x.decode() for x in f[b"path"]]
                files.append(TorrentFile(path_components, fn_len))
                total += fn_len
            length = total
        else:
            raise ValueError("Invalid .torrent file: missing length or files")

        return cls(raw, info_hash, piece_length, pieces, announce, name, length,
                   files=files, announce_list=announce_list)

    def generate_peer_id(self) -> bytes:
        return b"-PC0001-" + os.urandom(12)

    def num_pieces(self) -> int:
        return len(self.pieces)

    def is_multifile(self) -> bool:
        return self.files is not None

    def build_file_layout(self) -> List[Tuple[int, int, TorrentFile]]:
        if not self.is_multifile():
            raise RuntimeError("Not a multi-file torrent")

        layout = []
        offset = 0
        for f in self.files:
            start = offset
            end = offset + f.length
            layout.append((start, end, f))
            offset = end
        return layout

    def map_piece_to_file_ranges(self, index: int) -> List[Tuple[str, int, int]]:
        if not self.is_multifile():
            raise RuntimeError("Use single-file disk write instead")

        piece_start = index * self.piece_length
        piece_end = piece_start + self.piece_length

        ranges = []
        remaining = self.piece_length

        for start, end, f in self.build_file_layout():
            if end <= piece_start or start >= piece_end:
                continue

            overlap_start = max(piece_start, start)
            overlap_end = min(piece_end, end)
            length = overlap_end - overlap_start

            file_offset = overlap_start - start
            ranges.append((f.joined_path(), file_offset, length))

            remaining -= length
            if remaining <= 0:
                break

        return ranges
