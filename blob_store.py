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
from utils import Metadata
import hashlib
import tempfile
import os
import io
import json
import zipfile

class BlobStore:
    def __init__(self, path: str):
        self.base = Path(path)
        self.metadata =  Metadata()
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
        time = self.metadata.get_timestamp()
        content = data.file.read()
        name = data.filename
        H = hashlib.sha256(content).hexdigest()
        with tempfile.NamedTemporaryFile(dir=str(self.tmp_path), delete=False) as tf:
            tf.write(content)
        path = self.get_path(H)
        metadata = {}
        metadata["name"], metadata["type"] = self.metadata.get_name_and_type(name)
        metadata["hash"] = H
        os.replace(tf.name, path)
        metadata["size"] = self.metadata.get_size(path)
        metadata["timestamp"] = time
        meta_path = f"{path}_metadata"
        with open(meta_path, 'w') as f:
            f.write(str(metadata))
        return H

    def get(self, H: str) -> bytes:
        path = self.get_path(H)
        try:
            with open(path, 'rb') as f:
                return f.read()
        except FileNotFoundError:
            return "[Errno 2] No such file or directory: '{}'".format(path)
        
    def dummy_get(self, H:str):
        path = self.get_path(H)
        try:
            with open(path, 'rb') as f:
                return True
        except FileNotFoundError:
            return False
    
    def get_dataset(self, dataset_id: str):
        dataset_hash_dict = f"{dataset_id}.json"

        with open(dataset_hash_dict, 'r') as f:
            dataset_hash_dict = json.load(f)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            for k, v in dataset_hash_dict.items():
                try:
                    response = self.get(v)
                    zip_file.writestr(k, response)
                except Exception as e:
                    zip_file.writestr(hash + ".error", str(e))
        zip_buffer.seek(0)

        return zip_buffer
        
    def remove(self, H: str) -> None:
        path = self.get_path(H)
        os.remove(path)