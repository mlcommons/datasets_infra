from typing import Union

from fastapi import FastAPI, File, UploadFile
from blob_store import BlobStore

app = FastAPI()
path = ''
blob_store = BlobStore(path=path)

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/get_data")
def get_data(hash: str):
    return blob_store.get(hash)

@app.put("/put_data")
def put_data(file: UploadFile):
    return blob_store.put(file)
