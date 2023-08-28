import os
import datetime

class Metadata:
    def __init__(self) -> None:
        pass
    
    def get_timestamp(self)-> str:
        return datetime.datetime.now().strftime("%Y/%m/%d - %H:%M:%S")
    
    def get_name_and_type(self,filename: str):
        name = filename.split('.')[0]
        type = filename.split('.')[1]
        return name, type

    def get_size(self, path: str):
        return os.path.getsize(path)



