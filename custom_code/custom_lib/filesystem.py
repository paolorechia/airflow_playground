from custom_code.custom_lib import settings
from typing import Optional
import os
import json

class DataFilesystem:
    def __init__(self, data_dir_name: str):
        self._data_dir_name = data_dir_name
        self._path = os.path.join(settings.DATA_DIR, data_dir_name)
        try:
            os.makedirs(self._path)
        except FileExistsError:
            pass

    def path(self, filename: str) -> str:
        return os.path.join(self._path, filename)

    def write_dict_as_json(self, filename: str, data: dict):
        p = os.path.join(self._path, filename)
        with open(p, "w") as fp:
            json.dump(data, fp)
        
    def read_json(self, filename: str) -> Optional[dict]:
        p = os.path.join(self._path, filename)
        try:
            with open(p, "r") as fp:
                j = json.load(fp)
            return j
        except FileNotFoundError:
            return None