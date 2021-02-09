## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
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

from seecr.test import SeecrTestCase

from weightless.core import be, consume
from weightless.core.utils import asBytes
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
        consume(self.top.all.add(identifier="x", partname="part", data=b"<data/>"))
        self.assertEqual((True, True), self.top.call.isAvailable(identifier="x", partname="part"))

    def testDelete(self):
        consume(self.top.all.add(identifier="x", partname="part", data=b"<data/>"))
        consume(self.top.all.delete(identifier="x"))
        self.assertEqual((False, False), self.top.call.isAvailable(identifier="x", partname="part"))

    def testDeletePart(self):
        consume(self.top.all.add(identifier="x", partname="part1", data=b"<data/>"))
        consume(self.top.all.add(identifier="x", partname="part2", data=b"<data/>"))
        self.top.call.deletePart(identifier="x", partname="part1")
        self.assertEqual((False, False), self.top.call.isAvailable(identifier="x", partname="part1"))
        self.assertEqual((True, True), self.top.call.isAvailable(identifier="x", partname="part2"))

    def testGetStream(self):
        consume(self.top.all.add(identifier="x", partname="part1", data=b"<data/>"))
        self.assertEqual(b"<data/>", self.top.call.getStream(identifier="x", partname="part1").read())

    def testYieldRecord(self):
        consume(self.top.all.add(identifier="x", partname="part1", data=b"<data/>"))
        self.assertEqual(b"<data/>", asBytes(self.top.all.yieldRecord(identifier="x", partname="part1")))
        self.assertEqual(b"", asBytes(self.top.all.yieldRecord(identifier="y", partname="part1")))
