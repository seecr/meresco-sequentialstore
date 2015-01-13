## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from os import makedirs, listdir, rename, remove
from os.path import join, isdir, isfile
import sys

from _sequentialstoragebynum import _SequentialStorageByNum


imported = False
def lazyImport():
    global imported
    if imported:
        return
    imported = True
    from java.lang import Long
    from java.io import File
    from org.apache.lucene.search import NumericRangeQuery
    from meresco_sequentialstore import initVM as initMerescoSequentialStore
    initMerescoSequentialStore()
    from org.meresco.sequentialstore import SeqStorageIndex
    globals().update(locals())

from meresco.pylucene import getJVM
VM = getJVM()

class SequentialStorage(object):
    version = '1'

    def __init__(self, directory, commitCount=None):
        self._directory = directory
        self._versionFormatCheck()
        self._index = _Index(join(directory, INDEX_DIR))
        self._seqStorageByNum = _SequentialStorageByNum(join(directory, SEQSTOREBYNUM_NAME))
        self._lastKey = self._seqStorageByNum.lastKey or 0
        self._commitCount = 0
        self._maxCommitCount = commitCount or 1000

    def add(self, identifier, data):
        self._lastKey += 1
        key = self._lastKey
        self._seqStorageByNum.add(key=key, data=data)
        self._index[str(identifier)] = key  # only after actually writing data

    def delete(self, identifier):
        del self._index[str(identifier)]

    def __getitem__(self, identifier):
        key = self._index[str(identifier)]
        return self._seqStorageByNum[key]

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
        return ((keys2Identifiers.get(key), data) for key, data in result)

    def copyTo(self, target, skipDataCheck=False, verbose=False):
        self._seqStorageByNum.copyTo(target=target, keys=self._index.itervalues(), skipDataCheck=skipDataCheck, verbose=verbose)

    @classmethod
    def gc(cls, directory, skipDataCheck=False, verbose=False):
        """Works only for closed SequentialStorage for now."""
        if not isdir(join(directory, INDEX_DIR)) or not isfile(join(directory, SEQSTOREBYNUM_NAME)):
            raise ValueError('Directory %s does not belong to a %s.' % (directory, cls))
        s = cls(directory)
        tmpSeqStoreFile = join(directory, 'seqstore~')
        if isfile(tmpSeqStoreFile):
            remove(tmpSeqStoreFile)
        tmpSequentialStorageByNum = _SequentialStorageByNum(tmpSeqStoreFile)
        s.copyTo(tmpSequentialStorageByNum, skipDataCheck=skipDataCheck, verbose=verbose)
        s.close()
        tmpSequentialStorageByNum.close()
        rename(tmpSeqStoreFile, join(directory, 'seqstore'))
        if verbose:
            sys.stderr.write('Finished garbage-collecting SequentialStorage.')
            sys.stderr.flush()

    def close(self):
        if not getattr(self, '_seqStorageByNum', None) is None:
            self._seqStorageByNum.close()
            self._seqStorageByNum = None
        if not getattr(self, '_index', None) is None:
            self._index.close()
            self._index = None

    def _versionFormatCheck(self):
        versionFile = join(self._directory, "sequentialstorage.version")
        if isdir(self._directory):
            assert (listdir(self._directory) == []) or (isfile(versionFile) and open(versionFile).read() == self.version), "The SequentialStorage at %s needs to be converted to the current version." % self._directory
        else:
            assert not isfile(self._directory), 'Given directory name %s exists as file.' % self._directory
            makedirs(self._directory)
        with open(versionFile, 'w') as f:
            f.write(self.version)


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

    def itervalues(self):
        # WARNING: Performance penalty, forcefully reopens reader.
        self._reopen()
        iterable = self._index.itervalues()
        class IterableWithLength(object):
            def __init__(inner):
                inner.length = len(self)

            def __iter__(inner):
                return iterable

            def __len__(inner):
                return inner.length

        return IterableWithLength()

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


DELETED_RECORD = object()
INDEX_DIR = 'index'
SEQSTOREBYNUM_NAME = 'seqstore'
