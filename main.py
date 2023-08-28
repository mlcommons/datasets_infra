from typing import Union
import tempfile

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse, FileResponse
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

@app.get("/dummy_get")
def dummy_get(hash: str):
    return blob_store.dummy_get(hash)

@app.put("/put_data")
def put_data(file: UploadFile):
    return blob_store.put(file)

@app.get("/get_dataset")
def get_dataset(dataset_id: str):
    dataset = blob_store.get_dataset(dataset_id)
    return StreamingResponse(iter([dataset.getvalue()]), media_type="application/zip", headers={"Content-Disposition": "attachment; filename=files.zip"})

