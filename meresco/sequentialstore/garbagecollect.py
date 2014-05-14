from . import SequentialStorage


class GarbageCollect(object):
    def __init__(self, directory):
        self._directory = directory

    def collect(self):
        SequentialStorage.gc(self._directory)
