import os
import requests
import fire
import json
from multiprocessing import Pool

def get_all_file_paths(directory_path):
    file_paths = []
    for root, dirs, files in os.walk(directory_path):
        for filename in files:
            file_paths.append(os.path.join(root, filename))
    return file_paths

def upload_file(file_path):
    endpoint_url = 'http://127.0.0.1:8000/put_data'
    with open(file_path, 'rb') as f:
        files = {"file": (file_path, f, "multipart/form-data")}
        response = requests.put(endpoint_url, files=files)
        response_text = response.content.decode('utf-8').strip('"')
        return file_path, response_text

def main(directory_path, json_name):
    file_paths = get_all_file_paths(directory_path)
    with Pool() as p:
        results = p.map(upload_file, file_paths)
        p.close()
        p.join()
    response_dict = {file_path: response_text for file_path, response_text in results}

    with open(json_name, 'w') as f:
        json.dump(response_dict, f)

if __name__ == '__main__':
    fire.Fire(main)