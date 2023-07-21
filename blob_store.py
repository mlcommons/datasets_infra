'''
H = sha256(data)
path = H[:2] / H[2:4] / H

blob/aa/bb/H
tmp/*

to write a file to the store from python, allocate a temp file in tmp/ with NamedTemporaryFile(delete=False), then use os.replace to move it to `blob/path` once it is fully written to disk
to read from the store, try to open `path`
to reference a file, use the sha256 for now
'''

from pathlib import Path
import hashlib
import tempfile
import os

class BlobStore:
    def __init__(self, path: str):
        self.base = Path(path)
        self.blob_path = self.base / 'blob'
        self.tmp_path  = self.base / 'tmp'
        self.blob_path.mkdir(parents=True, exist_ok=True)
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        for p1 in range(256):
            d1 = self.blob_path / f"{p1:02x}" #zero-padded two-digit lowercase hexadecimal number.
            d1.mkdir(exist_ok=True)
            for p2 in range(256):
                d2 = d1 / f"{p2:02x}"
                d2.mkdir(exist_ok=True)

    def get_path(self, H: str) -> str:
        return self.blob_path / H[0:2] / H[2:4] / H

    def put(self, data: bytes) -> str:
        content = data.file.read()
        H = hashlib.sha256(content).hexdigest()
        print(H)
        with tempfile.NamedTemporaryFile(dir=str(self.tmp_path), delete=False) as tf:
            tf.write(content)
        path = self.get_path(H)
        os.replace(tf.name, path)
        return H

    def get(self, H: str) -> bytes:
        path = self.get_path(H)
        with open(path, 'rb') as f:
            return f.read()

    def remove(self, H: str) -> None:
        path = self.get_path(H)
        os.remove(path)

# if __name__ == '__main__':
#     import argparse
#     parser = argparse.ArgumentParser()
#     parser.add_argument('path')
#     args = parser.parse_args()

#     store = BlobStore(args.path)
#     H = store.put(b'test')
#     print(store.get(H))
#     store.remove(H)
#     store.get(H)
