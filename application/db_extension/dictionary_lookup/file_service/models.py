import joblib
import funcy
import pickle
from datetime import datetime
import os
from application.core.logging import logger
from pathlib import Path
from application.extensions import cache
from flask import current_app
from io import BytesIO

class BaseDataFile(object):
    def __init__(self, filename, preload=False, default_class=dict):
        if isinstance(filename, str):
            filename = Path(filename)
        self._filename = filename
        self._default_class = default_class
        self._data = default_class()
        self._loaded = False
        self._timestamp = self._get_actual_timestamp()
        if preload:
            self._load()

    def _get_actual_timestamp(self):
        if self._filename.exists():
            return os.path.getmtime(self._filename)
        else:
            return None

    def __getattr__(self, attr):
        def wrapped_method(*args, **kwargs):
            result = getattr(self._data, attr)(*args, **kwargs)
            return result

        if not self._loaded:
            self._load()
        if not hasattr(self._data, attr):
            print(attr)
            print(self._filename)
            raise AttributeError
        if attr.startswith('_'):
            return attr
        else:
            return wrapped_method

    def __getitem__(self, key):
        if not self._loaded:
            self._load()
        try:
            res = self._data[key]
        except KeyError:
            self._load()
            res = self._data[key]
        return res

    def __contains__(self, key):
        if not self._loaded:
            self._load()
        return key in self._data

    def is_changed(self):
        new_timestamp = self._get_actual_timestamp()
        if new_timestamp != self._timestamp:
            self._timestamp = new_timestamp
            return True
        else:
            return False

    #
    def _load(self):
        print('Loading: {}'.format(self._filename))
        try:
            self._data = self._load_function()
        except ValueError:
            logger.warning(f'Data not found for {self.key} perfoming dicitonary update')
            from application.dictionary_lookup import \
                update_dictionary_lookup_data
            update_dictionary_lookup_data(log_function=logger.info)
        finally:
            self._data = self._load_function()
        self._loaded = True
        return self._get_actual_timestamp()

    def dump(self, data):
        raise NotImplementedError
        # joblib.dump(self._data, self._filename)

    def _load_function(self):
        raise NotImplementedError


class JoblibDataFile(BaseDataFile):
    def _load_function(self):
        if self._filename.exists():
            return joblib.load(self._filename)
        else:
            logger.error("Can't open {}".format(self._filename))
            return self._default_class()

    def dump(self, data):
        if not self._filename.exists():
            self._filename.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(data, self._filename)
        self._data = data
        self._loaded = True


class PickleDataFile(BaseDataFile):
    def _load_function(self):
        result = None
        if self._filename.exists():
            with open(self._filename, 'rb') as f:
                try:
                    result = pickle.load(f)
                except EOFError as e:
                    logger.error("{} while loading: {}".format(e.__class__.__name__, self._filename))
        else:
            logger.error("File not found {}".format(self._filename))
        if result:
            return result
        else:
            return self._default_class()

    def dump(self, data):
        if not self._filename.exists():
            self._filename.parent.mkdir(parents=True, exist_ok=True)
        with open(self._filename, 'wb') as f:
            pickle.dump(data, f)
            self._data = data
            self._loaded = True


class CacheDataFile(BaseDataFile):
    def __init__(self, filename, preload=False, default_class=dict):
        self._filename = filename
        self._default_class = default_class
        self._data = default_class()
        self._loaded = False
        self._timestamp = self._get_actual_timestamp()
        if preload:
            self._load()

    def _get_actual_timestamp(self):
        with current_app.app_context():
            return cache.get(self.timestamp_key)

    def _set_timestamp(self):
        with current_app.app_context():
            cache.set(self.timestamp_key,
                      str(datetime.now()),
                      timeout=60*60*24*7)

    @property
    def key(self):
        return f"dump_{self._filename.name}"

    @property
    def timestamp_key(self):
        return f"{self.key}:time"

    def _load_function(self):
        result = None
        with current_app.app_context():
            _raw = cache.get(self.key)
            if _raw:
                result = pickle.loads(_raw)
            else:
                raise ValueError(f"Load data from redis: {self.key} - NOT FOUND")
            if result:
                return result
            else:
                return self._default_class()

    def dump(self, data):
        with current_app.app_context():
            cache.set(self.key, pickle.dumps(data), timeout=60*60*24*7)
            self._data = data
            self._loaded = True
            self._set_timestamp()

class PickleCacheDataFile(CacheDataFile):
    pass


class JoblibCacheDataFile(CacheDataFile):
    def _load_function(self):
        result = None
        with current_app.app_context():
            _raw = BytesIO(cache.get(self.key))
            if _raw:
                try:
                    result = joblib.load(_raw)
                except EOFError:
                    result = None
                logger.info(f"Load data from redis: {self.key} - SUCCESS")
            else:
                raise ValueError(
                    f"Load data from redis: {self.key} - NOT FOUND")
            if result:
                return result
            else:
                return self._default_class()

    def dump(self, data):
        with current_app.app_context():
            f = BytesIO()
            pickled = joblib.dump(data, f)
            cache.set(self.key, pickled, timeout=60*60*24*7)
            self._data = data
            self._loaded = True
            self._set_timestamp()
            logger.info(f"Dump data to redis: {self.key} - SUCCESS")
