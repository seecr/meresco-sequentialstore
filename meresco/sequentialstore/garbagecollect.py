from . import SequentialStorage


def garbageCollect(directory, skipDataCheck=False, verbose=False):
    SequentialStorage.gc(directory, skipDataCheck=skipDataCheck, verbose=verbose)
