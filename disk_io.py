import os


class DiskIO:
    # handles file io for pieces
    def __init__(self, path: str, total_length: int, preallocate: bool = True, truncate: bool = True):
        self.path = path
        self.total_length = total_length
        self.preallocate = preallocate
        self.truncate = truncate

        if not truncate and not os.path.exists(self.path):
            raise FileNotFoundError(f"Existing file required for seeding: {self.path}")

        mode = "wb+"
        if not truncate:
            mode = "r+b"

        self.file = open(self.path, mode)

        if self.preallocate and self.truncate:
            self._preallocate_file()

    def _preallocate_file(self):
        # grow file to full size
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
