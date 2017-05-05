
from org.meresco.sequentialstore import JSeqStoreIndex

DELETED_RECORD = object()
SEQSTOREBYNUM_NAME = 'seqstore'

class SeqStoreIndex(object):
    def __init__(self, path):
        self._index = JSeqStoreIndex(path)
        self._latestModifications = {}

    def __setitem__(self, key, value):
        assert value > 0  # 0 has special meaning in this context
        self._maybeReopen()
        self._index.setKeyValue(key, long(value))
        self._latestModifications[key] = value

    def __getitem__(self, key):
        value = self._latestModifications.get(key)
        if value == DELETED_RECORD:
            raise KeyError(key)
        elif value is not None:
            return value
        self._maybeReopen()
        value = self._index.getValue(key)
        if value == -1:
            raise KeyError(key)
        return value

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def reopen(self):
        self._index.reopen()

    def iterkeys(self):
        # WARNING: Performance penalty, forcefully reopens reader.
        self._reopen()
        return _IterableWithLength(self._index.iterkeys(), len(self))

    def itervalues(self):
        # WARNING: Performance penalty, forcefully reopens reader.
        self._reopen()
        return _IterableWithLength(self._index.itervalues(), len(self))

    def __len__(self):
        # WARNING: Performance penalty, commits writer to get an accurate length.
        self.commit()
        return self._index.length()

    def __delitem__(self, key):
        self._index.delete(key)
        self._latestModifications[key] = DELETED_RECORD

    def _maybeReopen(self):
        if len(self._latestModifications) > 10000:
            self._reopen()

    def _reopen(self):
        self._index.reopen()
        self._latestModifications.clear()

    def close(self):
        self._index.close()

    def commit(self):
        self._index.commit()


class _IterableWithLength(object):
    def __init__(self, iterable, length):
        self.iterable = iterable
        self.length = length

    def __iter__(self):
        return self.iterable

    def __len__(self):
        return self.length
