from config import DATA_DIR
from config import LOGGER
import json
import os
# import os

class LocalCache():
    def __init__(self, func, key, use_local_cache=False):
        self.func = func
        self.key = key
        self.use_local_cache = use_local_cache

    def fetch(self):
        if self.key_exists() and self.use_local_cache:
            LOGGER.debug("returning {} from cache".format(self.key))
            return self.get_json()
        data = self.func()
        self.cache_json(data)
        return data


    def key_exists(self):
        return os.path.isfile(self.file_path())

    def file_path(self):
        return os.path.join(DATA_DIR, "{}.json".format(self.key))

    def get_json(self):
        with open(self.file_path()) as json_file:
            return json.load(json_file)

    def cache_json(self, data):
        with open(self.file_path(), 'w') as json_file:
            json.dump(data, json_file)


