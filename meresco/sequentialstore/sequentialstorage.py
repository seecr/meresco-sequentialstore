## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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
from os.path import join, isdir, isfile, getsize
from warnings import warn

from .export import Export

try:
    from org.meresco.sequentialstore import StoreLucene
    from lucene import JArray, JavaError
    from org.apache.lucene.util import BytesRef
except ImportError:
    raise ImportError("initVM() not called: please add to your project: 'from lucene import initVM; initVM(); from meresco_sequentialstore import initVM; initVM()'")

class SequentialStorage(object):
    version = '5'

    def __init__(self, directory, maxModifications=None):
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
        if not isinstance(data, bytes):
            raise TypeError('data should be bytes')
        identifier = str(identifier)
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
        "Note: must not be called in inner loop of bulk processing, because of commit"
        self.commit()  # not found a sure way yet to prevent this necessity
        return self._luceneStore.numDocs()

    def iterkeys(self):
        self.commit()
        return self._luceneStore.iterkeys()

    __iter__ = iterkeys

    def iteritems(self):
        self.commit()
        return ((item.identifier, _toBytes(item.data)) for item in self._luceneStore.iteritems())

    def itervalues(self):
        return (_toBytes(item.data) for item in self._luceneStore.iteritems())

    def commit(self):
        self._luceneStore.commit()
        self._reopen()

    def export(self, exportPath):
        Export(exportPath).export(self)

    def importFrom(self, importPath):
        Export(importPath).importInto(self)

    def close(self):
        if self._luceneStore is None:
            return
        self._luceneStore.commit()
        self._luceneStore.close()
        self._luceneStore = None

    def gc(self, maxNumSegments=1, doWait=False):
        "Note: to prevent from potentially crashing on 'disk full' during active GC, a client needs to take care of handling (ignoring?) IOException."
        try:
            self._luceneStore.forceMerge(maxNumSegments, doWait)
            if doWait:
                self.commit()
        except JavaError as e:
            original = e.getJavaException()
            if original.getClass().getName() == 'IOException':
                raise IOError(original.getMessage())
            raise

    def getSizeOnDisk(self):
        path = self._directory
        return sum(getsize(join(path, f)) for f in listdir(path) if isfile(join(path, f)))

    def _getData(self, identifier):
        byteArray = self._luceneStore.getData(identifier)
        return _toBytes(byteArray)

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
                with open(versionFile) as fp:
                    assert fp.read() == self.version, "The SequentialStorage at %s needs to be converted to the current version." % self._directory
            else:
                assert (listdir(self._directory) == []), "The %s directory is already in use for something other than a SequentialStorage." % self._directory
        else:
            assert not isfile(self._directory), 'Given directory name %s exists as file.' % self._directory
            makedirs(self._directory)
        with open(versionFile, 'w') as f:
            f.write(self.version)


_DEFAULT_MAX_MODIFICATIONS = 10000
_DELETED_RECORD = object()

def _toBytes(bytesRef):
    return None if bytesRef is None else bytes([i & 0xff for i in bytesRef.bytes])

