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

from os import makedirs, listdir
from os.path import join, isdir, isfile

from _sequentialstoragebynum import _SequentialStorageByNum
from collections import namedtuple


class ForConversionOnlyV2SequentialStorage(object):
    version = '2'

    def __init__(self, directory, commitCount=None):
        self._directory = directory
        self._versionFormatCheck()
        seqStoreByNumFileName = join(directory, SEQSTOREBYNUM_NAME)
        self._seqStorageByNum = _SequentialStorageByNum(seqStoreByNumFileName)
        self._lastKey = self._seqStorageByNum.lastKey or 0

    def add(self, identifier, data):
        self._lastKey += 1
        key = self._lastKey
        data = self._wrap(identifier, data)
        self._seqStorageByNum.add(key=key, data=data)

    def delete(self, identifier):
        self._lastKey += 1
        key = self._lastKey
        data = self._wrap(identifier, delete=True)
        self._seqStorageByNum.add(key=key, data=data)

    def close(self):
        if self._seqStorageByNum is None:
            return
        self._seqStorageByNum.close()
        self._seqStorageByNum = None

    def commit(self):
        self._seqStorageByNum.flush()

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

SEQSTOREBYNUM_NAME = 'seqstore'
