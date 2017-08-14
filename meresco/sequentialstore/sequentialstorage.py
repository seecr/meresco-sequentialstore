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

from os import getenv, makedirs, listdir
from os.path import join, isdir, isfile
from warnings import warn
from itertools import islice


def importVM():
    maxheap = getenv('PYLUCENE_MAXHEAP')
    if not maxheap:
        maxheap = '4g'
        warn("Using '4g' as maxheap for lucene.initVM(). To override use PYLUCENE_MAXHEAP environment variable.")
    from lucene import initVM, getVMEnv
    try:
        VM = initVM(maxheap=maxheap)
        # VM = initVM(maxheap=maxheap, vmargs='-agentlib:hprof=heap=sites')
    except ValueError:
        VM = getVMEnv()
    return VM


imported = False
JArray = None
BytesRef = None
StoreLucene = None

def lazyImport():
    global imported
    if imported:
        return
    importVM()

    from meresco_sequentialstore import initVM as initMerescoSequentialStore
    initMerescoSequentialStore()
    from org.meresco.sequentialstore import StoreLucene
    from lucene import JArray
    from org.apache.lucene.util import BytesRef
    imported = True
    globals().update(locals())


class SequentialStorage(object):
    version = '3'

    def __init__(self, directory, maxModifications=None):
        lazyImport()
        self._directory = directory
        if not isdir(directory):
            makedirs(directory)
        self._versionFormatCheck()
        self._maxModifications = DEFAULT_MAX_MODIFICATIONS if maxModifications is None else maxModifications
        self._luceneStore = StoreLucene(directory)
        self._latestModifications = {}
        self.gets = 0
        self.cacheHits = 0

    def add(self, identifier, data):
        if identifier is None:
            raise ValueError('identifier should not be None')
        if data is None:
            raise ValueError('data should not be None')
        identifier = str(identifier)
        data = str(data)
        self._luceneStore.add(identifier, pyStrToBytesRef(data))
        self._latestModifications[identifier] = data
        if len(self._latestModifications) > self._maxModifications:
            self.commit()

    __setitem__ = add

    def delete(self, identifier):
        identifier = str(identifier)
        self._luceneStore.delete(identifier)
        self._latestModifications[identifier] = DELETED_RECORD

    __delitem__ = delete

    def __getitem__(self, identifier):
        self.gets += 1
        identifier = str(identifier)
        value = self._latestModifications.get(identifier)
        if not value is None:
            self.cacheHits += 1
            if value is DELETED_RECORD:
                raise KeyError(identifier)
            return value
        data = self._getData(identifier)
        if data is None:
            raise KeyError(identifier)
        return str(data)

    def get(self, identifier, default=None):
        try:
            return self[identifier]
        except KeyError:
            return default

    def getMultiple(self, identifiers, ignoreMissing=False):
        for identifier in identifiers:
            identifier = str(identifier)
            try:
                data = self[identifier]
            except KeyError:
                if ignoreMissing:
                    continue
                raise
            yield identifier, data

    def __len__(self):
        return self._luceneStore.numDocs()

    def iterkeys(self):
        self.commit()
        return self._luceneStore.iterkeys()

    __iter__ = iterkeys

    def iteritems(self):
        self.commit()
        return ((key, self._getData(key)) for key in self._luceneStore.iterkeys())

    def itervalues(self):
        self.commit()
        return (self._getData(key) for key in self._luceneStore.iterkeys())

    def close(self):
        if self._luceneStore is None:
            return
        self.commit()
        self._luceneStore.close()
        self._luceneStore = None

    def commit(self):
        self._luceneStore.commit()
        self._reopen()

    def _getData(self, identifier):
        if str(identifier) not in self._latestModifications and len(self._latestModifications) > self._maxModifications:
            self._reopen()
        data = self._luceneStore.getData(identifier)
        if data is None:
            return None
        dataBytesRef = self._luceneStore.getData(identifier)
        return bytesRefToPyStr(dataBytesRef) if not dataBytesRef is None else None

    def _reopen(self):
        self._luceneStore.reopen()
        self._latestModifications.clear()

    def _versionFormatCheck(self):
        versionFile = join(self._directory, "sequentialstorage.version")
        if isdir(self._directory):
            assert (listdir(self._directory) == []) or (isfile(versionFile) and open(versionFile).read() == self.version), "The SequentialStorage at %s needs to be converted to the current version." % self._directory
        else:
            assert not isfile(self._directory), 'Given directory name %s exists as file.' % self._directory
            makedirs(self._directory)
        with open(versionFile, 'w') as f:
            f.write(self.version)


def pyStrToBytesRef(s):
     return BytesRef(JArray('byte')(s))

def bytesRefToPyStr(bytesRef):
    return ''.join(chr(b & 0xFF) for b in islice(bytesRef.bytes, bytesRef.offset, bytesRef.length))


DEFAULT_MAX_MODIFICATIONS = 10000
DELETED_RECORD = object()
