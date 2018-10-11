## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014-2017 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015 Stichting Kennisnet http://www.kennisnet.nl
#
# This file is part of "Meresco SequentialStore"
#
# "Meresco SequentialStore" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "Meresco SequentialStore" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "Meresco SequentialStore"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

import sys
from os import getenv, makedirs, listdir, rename, remove
from os.path import join, isdir, isfile
from shutil import rmtree
from warnings import warn

from _sequentialstoragebynum import _SequentialStorageByNum
from collections import namedtuple


imported = False
SeqStorageIndex = None
def lazyImport():
    global imported
    if imported:
        return
    imported = True
    from meresco_sequentialstore import initVM as initMerescoSequentialStore
    initMerescoSequentialStore()
    from org.meresco.sequentialstore import SeqStorageIndex
    globals().update(locals())

def importVM():
    maxheap = getenv('PYLUCENE_MAXHEAP')
    if not maxheap:
        maxheap = '4g'
        warn("Using '4g' as maxheap for lucene.initVM(). To override use PYLUCENE_MAXHEAP environment variable.")
    from lucene import initVM, getVMEnv
    try:
        VM = initVM(maxheap=maxheap)#, vmargs='-agentlib:hprof=heap=sites')
    except ValueError:
        VM = getVMEnv()
    return VM
importVM()

from .export import Export


class SequentialStorage(object):
    version = '2'

    def __init__(self, directory, commitCount=None):
        self._directory = directory
        self._versionFormatCheck()
        indexDir = join(directory, INDEX_DIR)
        seqStoreByNumFileName = join(directory, SEQSTOREBYNUM_NAME)
        if isfile(seqStoreByNumFileName) and not isdir(indexDir):
            self.recoverIndexFromData(directory, verbose=True)
        self._index = _Index(indexDir)
        self._seqStorageByNum = _SequentialStorageByNum(seqStoreByNumFileName)
        self._lastKey = self._seqStorageByNum.lastKey or 0

    def add(self, identifier, data):
        self._lastKey += 1
        key = self._lastKey
        data = self._wrap(identifier, data)
        self._seqStorageByNum.add(key=key, data=data)
        self._index[str(identifier)] = key  # only after actually writing data

    def delete(self, identifier):
        self._lastKey += 1
        key = self._lastKey
        data = self._wrap(identifier, delete=True)
        self._seqStorageByNum.add(key=key, data=data)
        del self._index[str(identifier)]

    def __getitem__(self, identifier):
        key = self._index[str(identifier)]
        data = self._seqStorageByNum[key]
        return self._unwrap(data).data

    def get(self, identifier, default=None):
        try:
            return self[identifier]
        except KeyError:
            return default

    def getMultiple(self, identifiers, ignoreMissing=False):
        keys2Identifiers = dict()
        for identifier in identifiers:
            identifier = str(identifier)
            try:
                key = self._index[identifier]
            except KeyError:
                if not ignoreMissing:
                    raise
            else:
                keys2Identifiers[key] = identifier
        result = self._seqStorageByNum.getMultiple(keys=sorted(keys2Identifiers.keys()), ignoreMissing=ignoreMissing)
        return ((keys2Identifiers.get(key), self._unwrap(data).data) for key, data in result)

    def copyTo(self, target, skipDataCheck=False, verbose=False):
        self._seqStorageByNum.copyTo(target=target, keys=self._index.itervalues(), skipDataCheck=skipDataCheck, verbose=verbose)

    @classmethod
    def gc(cls, directory, targetDir=None, skipDataCheck=False, verbose=False):
        """Works only for closed SequentialStorage for now."""
        if not isdir(join(directory, INDEX_DIR)) or not isfile(join(directory, SEQSTOREBYNUM_NAME)):
            raise ValueError('Directory %s does not belong to a %s.' % (directory, cls))
        targetDir = targetDir or directory
        if not isdir(targetDir):
            raise ValueError("'targetDir' %s is not an existing directory." % targetDir)
        s = cls(directory)
        tmpSeqStoreFile = join(targetDir, 'seqstore~')
        if isfile(tmpSeqStoreFile):
            remove(tmpSeqStoreFile)
        tmpSequentialStorageByNum = _SequentialStorageByNum(tmpSeqStoreFile)
        s.copyTo(tmpSequentialStorageByNum, skipDataCheck=skipDataCheck, verbose=verbose)
        s.close()
        tmpSequentialStorageByNum.close()
        rename(tmpSeqStoreFile, join(targetDir, 'seqstore'))
        if verbose:
            if directory == targetDir:
                sys.stderr.write('Finished garbage-collecting SequentialStorage.\n\n')
            else:
                sys.stderr.write("To finish garbage-collecting the SequentialStorage, now replace '%s' with '%s' manually.\n\n" % (join(directory, 'seqstore'), join(targetDir, 'seqstore')))
            sys.stderr.flush()

    def close(self):
        if self._seqStorageByNum is None:
            return
        self._seqStorageByNum.close()
        self._seqStorageByNum = None
        self._index.close()
        self._index = None

    def commit(self):
        self._seqStorageByNum.flush()
        self._index.commit()

    @classmethod
    def recoverIndexFromData(cls, directory, verbose=False):
        indexDir = join(directory, INDEX_DIR)
        assert not isdir(indexDir), "To allow for recovery, the index directory '%s' should be removed first." % indexDir
        tmpIndexDir = join(directory, INDEX_DIR + '.tmp')
        if isdir(tmpIndexDir):
            rmtree(tmpIndexDir)
        index = _Index(tmpIndexDir)
        seqStorageByNum = _SequentialStorageByNum(join(directory, SEQSTOREBYNUM_NAME))
        count = 0
        for key, data in seqStorageByNum.range():
            count += 1
            if verbose and count % 2000 == 0:
                sys.stderr.write('\rRecovered %s items, current key: %s, last key: %s' % (count, key, seqStorageByNum.lastKey))
                sys.stderr.flush()
            event = cls._unwrap(data)
            identifier = str(event.identifier)
            if event.delete:
                del index[identifier]
            else:
                index[identifier] = key
        index.close()
        rename(tmpIndexDir, indexDir)
    
    def export(self, exportPath):
        Export(exportPath).export(self)

    def importFrom(self, importPath):
        Export(importPath).importInto(self)


    def events(self):
        for key, data in iter(self._seqStorageByNum):
            yield self._unwrap(data)

    @staticmethod
    def _wrap(identifier, data=None, delete=False):
        if '\n' in identifier:
            raise ValueError("'\\n' not allowed in identifier " + repr(identifier))
        return "%s%s\n%s" % ("-" if delete else "+", identifier, data or '')

    @staticmethod
    def _unwrap(data):
        header, data = data.split('\n', 1)
        delete = header[0] == '-'
        identifier = header[1:]
        return Event(identifier, data, delete)

    def _versionFormatCheck(self):
        versionFile = join(self._directory, "sequentialstorage.version")
        if isdir(self._directory):
            assert (listdir(self._directory) == []) or (isfile(versionFile) and open(versionFile).read() == self.version), "The SequentialStorage at %s needs to be converted to the current version." % self._directory
        else:
            assert not isfile(self._directory), 'Given directory name %s exists as file.' % self._directory
            makedirs(self._directory)
        with open(versionFile, 'w') as f:
            f.write(self.version)

Event = namedtuple('Event', ['identifier', 'data', 'delete'])

class _Index(object):
    def __init__(self, path):
        lazyImport()
        self._index = SeqStorageIndex(path)
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


DELETED_RECORD = object()
INDEX_DIR = 'index'
SEQSTOREBYNUM_NAME = 'seqstore'
