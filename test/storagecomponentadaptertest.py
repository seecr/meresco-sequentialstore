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

from weightless.core import be, consume, asString
from meresco.core import Observable

from meresco.sequentialstore import StorageComponentAdapter, MultiSequentialStorage



class StorageComponentAdapterTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        storageComponentAdapter = StorageComponentAdapter()
        multiSequentialStorage = MultiSequentialStorage(self.tempdir)
        self.top = be(
            (Observable(),
                (storageComponentAdapter,
                    (multiSequentialStorage,)
                )
            )
        )

    def testAdd(self):
        consume(self.top.all.add(identifier="x", partname="part", data="<data/>"))
        self.assertEquals((True, True), self.top.call.isAvailable(identifier="x", partname="part"))

    def testDelete(self):
        consume(self.top.all.add(identifier="x", partname="part", data="<data/>"))
        consume(self.top.all.delete(identifier="x"))
        self.assertEquals((False, False), self.top.call.isAvailable(identifier="x", partname="part"))

    def testDeletePart(self):
        consume(self.top.all.add(identifier="x", partname="part1", data="<data/>"))
        consume(self.top.all.add(identifier="x", partname="part2", data="<data/>"))
        self.top.call.deletePart(identifier="x", partname="part1")
        self.assertEquals((False, False), self.top.call.isAvailable(identifier="x", partname="part1"))
        self.assertEquals((True, True), self.top.call.isAvailable(identifier="x", partname="part2"))

    def testGetStream(self):
        consume(self.top.all.add(identifier="x", partname="part1", data="<data/>"))
        self.assertEquals("<data/>", self.top.call.getStream(identifier="x", partname="part1").read())

    def testYieldRecord(self):
        consume(self.top.all.add(identifier="x", partname="part1", data="<data/>"))
        self.assertEquals("<data/>", asString(self.top.all.yieldRecord(identifier="x", partname="part1")))
