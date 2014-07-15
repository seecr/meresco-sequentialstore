## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
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
