# disk_io.py
import os

class DiskIO:
    """
    Handles reading/writing pieces to the target file for single-file torrents.
    Expected usage:
        disk = DiskIO("ubuntu.iso", total_length)
        disk.write_piece(index, data, piece_length)
        disk.finalize()
    """
    def __init__(self, path: str, total_length: int, preallocate: bool = True):
        self.path = path
        self.total_length = total_length
        self.preallocate = preallocate

        self.file = open(self.path, "wb+")

        if self.preallocate:
            self._preallocate_file()

    def _preallocate_file(self):
        self.file.seek(self.total_length - 1)
        self.file.write(b"\x00")
        self.file.flush()

    def write_piece(self, index: int, data: bytes, piece_length: int):
        offset = index * piece_length
        if offset + len(data) > self.total_length:
            raise ValueError(f"write would exceed file size at piece {index}")

        self.file.seek(offset)
        self.file.write(data)
        self.file.flush()

    def read_piece(self, index: int, piece_length: int) -> bytes:
        offset = index * piece_length
        self.file.seek(offset)
        return self.file.read(piece_length)

    def finalize(self):
        if not self.file.closed:
            self.file.flush()
            self.file.close()

    def __del__(self):
        try:
            self.finalize()
        except:
            pass

