## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014-2018 Seecr (Seek You Too B.V.) http://seecr.nl
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
from base64 import standard_b64decode


class SequentialStorage(object):
    version = '4'

    def __init__(self, directory, maxModifications=None):
        _importFromJava()
        self._directory = directory
        if not isdir(directory):
            makedirs(directory)
        self._versionFormatCheck()
        self._maxModifications = _DEFAULT_MAX_MODIFICATIONS if maxModifications is None else maxModifications
        self._luceneStore = StoreLucene(directory)
        self._latestModifications = {}

    def add(self, identifier, data):
        if identifier is None:
            raise ValueError('identifier should not be None')
        if data is None:
            raise ValueError('data should not be None')
        identifier = str(identifier)
        data = str(data)
        self._luceneStore.add(identifier, BytesRef(JArray('byte')(data)))
        self._latestModifications[identifier] = data
        self._maybeCommit()

    __setitem__ = add

    def delete(self, identifier):
        identifier = str(identifier)
        self._luceneStore.delete(identifier)
        self._latestModifications[identifier] = _DELETED_RECORD
        self._maybeCommit()

    __delitem__ = delete

    def __getitem__(self, identifier):
        identifier = str(identifier)
        value = self._latestModifications.get(identifier)
        if not value is None:
            if value is _DELETED_RECORD:
                raise KeyError(identifier)
            return value
        data = self._getData(identifier)
        if data is None:
            raise KeyError(identifier)
        return data

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
        return ((item.identifier, standard_b64decode(item.data)) for item in self._luceneStore.iteritems())

    def itervalues(self):
        self.commit()
        return (standard_b64decode(data) for data in self._luceneStore.itervalues())

    def commit(self):
        self._luceneStore.commit()
        self._reopen()

    def close(self):
        if self._luceneStore is None:
            return
        self._luceneStore.commit()
        self._luceneStore.close()
        self._luceneStore = None

    def gc(self, maxNumSegments=1, doWait=False):
        self._luceneStore.forceMerge(maxNumSegments, doWait)
        if doWait:
            self.commit()

    def _getData(self, identifier):
        b64encodedData = self._luceneStore.getData(identifier)
        return standard_b64decode(b64encodedData) if not b64encodedData is None else None

    def _maybeCommit(self):
        if len(self._latestModifications) > self._maxModifications:
            self.commit()

    def _reopen(self):
        self._luceneStore.reopen()
        self._latestModifications.clear()

    def _versionFormatCheck(self):
        versionFile = join(self._directory, "sequentialstorage.version")
        if isdir(self._directory):
            if isfile(versionFile):
                assert open(versionFile).read() == self.version, "The SequentialStorage at %s needs to be converted to the current version (with sequentialstore_convert_v2_to_v3)." % self._directory
            else:
                assert (listdir(self._directory) == []), "The %s directory is already in use for something other than a SequentialStorage." % self._directory
        else:
            assert not isfile(self._directory), 'Given directory name %s exists as file.' % self._directory
            makedirs(self._directory)
        with open(versionFile, 'w') as f:
            f.write(self.version)


_DEFAULT_MAX_MODIFICATIONS = 10000
_DELETED_RECORD = object()

imported = False
JArray = None
BytesRef = None
StoreLucene = None

def _importFromJava():
    global imported
    if imported:
        return
    _importVM()
    from meresco_sequentialstore import initVM as initMerescoSequentialStore
    initMerescoSequentialStore()
    from org.meresco.sequentialstore import StoreLucene
    from lucene import JArray
    from org.apache.lucene.util import BytesRef
    globals().update(locals())
    imported = True

def _importVM():
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
