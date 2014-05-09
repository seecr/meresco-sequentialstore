from os.path import join, isdir
from os import listdir, makedirs
from escaping import escapeFilename

from _sequentialstoragebynum import _SequentialStorageByNum


class MultiSequentialStorage(object):
    def __init__(self, path, name=None):
        self._path = path
        self._name = name
        isdir(self._path) or makedirs(self._path)
        self._storage = {}
        for name in listdir(path):
            self._getStorage(name)

    def observable_name(self):
        return self._name

    def _getStorage(self, name):
        storage = self._storage.get(name)
        if not storage:
            name = escapeFilename(name)
            self._storage[name] = storage = _SequentialStorageByNum(join(self._path, name))
        return storage

    def add(self, identifier, partname, data):
        self.addData(key=identifier, name=partname, data=data)
        return
        yield

    def addData(self, key, name, data):
        self._getStorage(name).add(key, data)

    def getData(self, key, name):
        return self._getStorage(name)[key]

    def iterData(self, name, start, stop=None, **kwargs):
        return self._getStorage(name).range(start, stop=stop, **kwargs)

    def getMultipleData(self, name, keys, ignoreMissing=False):
        return self._getStorage(name).getMultiple(keys, ignoreMissing=ignoreMissing)

    def handleShutdown(self):
        print 'handle shutdown: saving SequentialMultiStorage %s' % self._path
        from sys import stdout; stdout.flush()
        self.flush()

    def flush(self):
        for storage in self._storage.itervalues():
            storage.flush()