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

from seecr.test import SeecrTestCase

from weightless.core import be, consume
from meresco.core import Observable

from meresco.sequentialstore import AddDeleteToMultiSequential, MultiSequentialStorage


class AddDeleteToMultiSequentialTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        addDeleteToMultiSequential = AddDeleteToMultiSequential()
        self.multiSequentialStorage = MultiSequentialStorage(self.tempdir)
        self.top = be(
            (Observable(),
                (addDeleteToMultiSequential,
                    (self.multiSequentialStorage,)
                )
            )
        )

    def testAdd(self):
        consume(self.top.all.add(identifier="x", partname="part", data="<data/>"))
        self.assertEquals('<data/>', self.multiSequentialStorage.getData(identifier='x', name="part"))

    def testDelete(self):
        consume(self.top.all.add(identifier="x", partname="part", data="<data/>"))
        consume(self.top.all.delete(identifier="x"))
        self.assertRaises(KeyError, lambda: self.multiSequentialStorage.getData(identifier='x', name='part'))
