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

from collections import namedtuple
from lucene import JArray

from .seqstoreindex import SeqStoreIndex
from .seqstorestore import SeqStoreStore

INDEX_DIR = 'index'

class SequentialStorage(object):
    version = '2'

    def __init__(self, directory, commitCount=None):
        self._directory = directory
        self._versionFormatCheck()
        indexDir = join(directory, INDEX_DIR)
        self._index = SeqStoreIndex(indexDir)
        self._store = SeqStoreStore(directory)
        self._lastKey = self._store.lastKey or 0

    def add(self, identifier, data):
        self._lastKey += 1
        key = self._lastKey
        data = self._wrap(identifier, data)
        self._store.add(key, data)
        self._index[str(identifier)] = key  # only after actually writing data

    def delete(self, identifier):
        self._lastKey += 1
        key = self._lastKey
        data = self._wrap(identifier, delete=True)
        self._store.add(key, data)
        del self._index[str(identifier)]

    def __getitem__(self, identifier):
        key = self._index[str(identifier)]
        data = self._store.get(int(key))
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
                keys2Identifiers[int(key)] = identifier
        jarray = JArray('int')(sorted(keys2Identifiers.keys()))
        result = self._store.getMultiple(jarray, ignoreMissing)
        return ((keys2Identifiers.get(int(key)), self._unwrap(data).data) for key, data in result)

    def gc(self, verbose=False):
        self._index.reopen()
        current_keys = self._index._index.current_keys()
        self._store._store.delete_all_but(current_keys)
        self._store.reopen()

    def commit(self):
        self._store.flush()
        self._index.commit()

    def close(self):
        self.commit()
        self._store.close()
        self._index.close()

    def events(self):
        self._store.reopen();
        for event in self._store.list_events():
            yield self._unwrap(event.data)

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

