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


from cStringIO import StringIO

from meresco.core import Observable


class StorageComponentAdapter(Observable):
    """Provided for 'backwards' compatibility to allow MultiSequentialStorage to be accessed by older components (in Meresco DNA)."""

    def add(self, identifier, partname, data):
        self.call.addData(identifier=identifier, name=partname, data=data)
        return
        yield

    def delete(self, identifier):
        self.call.deleteData(identifier=identifier)
        return
        yield

    def deletePart(self, identifier, partname):
        self.call.deleteData(identifier=identifier, name=partname)

    def isAvailable(self, identifier, partname):
        try:
            self.call.getData(identifier=identifier, name=partname)
            return True, True
        except KeyError:
            return False, False

    def getStream(self, identifier, partname):
        return StringIO(self.call.getData(identifier=identifier, name=partname))

    def yieldRecord(self, identifier, partname):
        yield self.call.getData(identifier=identifier, name=partname)
