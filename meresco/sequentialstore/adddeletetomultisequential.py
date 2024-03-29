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

from meresco.core import Transparent


class AddDeleteToMultiSequential(Transparent):
    """Provided for 'backwards' compatibility to allow MultiSequentialStorage to be passed 'add' and 'delete' messages by older components (in Meresco DNA)."""
    def add(self, identifier, partname, data):
        if not type(data) is bytes:
            data = bytes(data, encoding="utf-8")
        self.call.addData(identifier=identifier, name=partname, data=data)
        return
        yield

    def delete(self, identifier):
        self.call.deleteData(identifier=identifier)
        return
        yield
