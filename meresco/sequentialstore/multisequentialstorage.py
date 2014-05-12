from os.path import join, isdir
from os import listdir, makedirs
from escaping import escapeFilename

from sequentialstorage import SequentialStorage


class MultiSequentialStorage(object):
    def __init__(self, directory, name=None):
        self._directory = directory
        self._name = name
        isdir(self._directory) or makedirs(self._directory)
        self._storage = {}
        for name in listdir(directory):
            self._getStorage(name)

    def observable_name(self):
        return self._name

    def addData(self, identifier, name, data):
        self._getStorage(name).add(identifier, data)

    def deleteData(self, identifier, name=None):
        if name is None:
            for storage in self._storage.values():
                storage.delete(identifier)
        else:
            self._getStorage(name).delete(identifier)

    def getData(self, identifier, name):
        return self._getStorage(name)[identifier]

    def getMultipleData(self, name, identifiers, ignoreMissing=False):
        return self._getStorage(name).getMultiple(identifiers, ignoreMissing=ignoreMissing)

    def handleShutdown(self):
        print 'handle shutdown: saving MultiSequentialStorage %s' % self._directory
        from sys import stdout; stdout.flush()
        self.close()

    def close(self):
        for storage in self._storage.itervalues():
            storage.close()

    def _getStorage(self, name):
        storage = self._storage.get(name)
        if not storage:
            name = escapeFilename(name)
            self._storage[name] = storage = SequentialStorage(join(self._directory, name))
        return storage
