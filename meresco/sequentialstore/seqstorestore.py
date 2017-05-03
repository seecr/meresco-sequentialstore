from org.meresco.sequentialstore import JSeqStoreStore
from meresco_sequentialstore import JavaError, InvalidArgsError #TODO yuk

class SeqStoreStore(object):
    def __init__(self, directory):
        self._store = JSeqStoreStore(directory)
        self.lastKey = self._store.lastKey()

    def add(self, key, data):
        if not isinstance(key, int):
            raise ValueError(key)
        if key <= self.lastKey:
            raise ValueError("key %s must be greater than last key %s" % (key, self.lastKey))
        self._store.add(key, data)
        self.lastKey = key

    def close(self):
        self._store.close()
    
    def get(self, key):
        self._store.reopen()
        if not isinstance(key, int):
            raise ValueError('Expected int')
        try:
            return self._store.get(key)
        except JavaError, e:
            m = e.getJavaException().getMessage()
            if "IndexError" in m:
                raise IndexError(int(m.split(',')[1]))
            raise

    def __getitem__(self, key):
        return self.get(key)

    def getMultiple(self, keys, ignore_missing=False):
        self._store.reopen();
        try:
            results = self._store.getMultiple(keys, ignore_missing)
        except InvalidArgsError, e:
            raise ValueError("Expected int")
        else:
            try:
                for data in results:
                    key, value = data.split('\n', 1)
                    yield int(key), value
            except JavaError, e:
                m = e.getJavaException().getMessage()
                if "IndexError" in m:
                    raise KeyError(int(m.split(',')[1]))
                if "Keys should have been sorted." in m:
                    raise ValueError("Keys should have been sorted.")
                raise

    def flush(self):
        self._store.flush()

    def reopen(self):
        self._store.reopen()

    def list_events(self):
        return self._store.list_events()

    def range(self, start_key, end_key=-1, inclusive=False):
        self._store.reopen()
        for e in self._store.range(start_key, end_key, inclusive):
            yield e.key, e.data
