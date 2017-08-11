## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2014, 2017 Seecr (Seek You Too B.V.) http://seecr.nl
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

from os import remove
from os.path import join, isdir

from meresco.sequentialstore import MultiSequentialStorage, SequentialStorage


class MultiSequentialStorageTest(SeecrTestCase):
    def testSequentialStoragePerPart(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData('1', "oai_dc", "<data/>")
        s.addData(identifier='2', name="rdf", data="<rdf/>")
        s.close()
        ss = SequentialStorage(join(self.tempdir, 'oai_dc'))
        self.assertEquals('<data/>', ss['1'])
        ss = SequentialStorage(join(self.tempdir, 'rdf'))
        self.assertEquals('<rdf/>', ss['2'])

    def testGetForUnknownPart(self):
        s = MultiSequentialStorage(self.tempdir)
        self.assertRaises(KeyError, lambda: s.getData('42', 'oai_dc'))

    def testGetForUnknownIdentifier(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData('1', "oai_dc", "x")
        self.assertRaises(KeyError, lambda: s.getData('42', 'oai_dc'))

    def testReadWriteData(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData('1', "oai_dc", "<data/>")
        s.close()
        sReopened = MultiSequentialStorage(self.tempdir)
        self.assertEquals('<data/>', sReopened.getData('1', 'oai_dc'))

    def testReadWriteIdentifier(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData('1', "oai_dc", "<data>1</data>")
        s.addData('2', "oai_dc", "<data>2</data>")
        s.close()
        sReopened = MultiSequentialStorage(self.tempdir)
        self.assertEquals('<data>1</data>', sReopened.getData(1, 'oai_dc'))
        self.assertEquals('<data>2</data>', sReopened.getData(2, 'oai_dc'))

    def testMonotonicityNotRequiredOverDifferentParts(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData('2', "oai_dc", "<data/>")
        s.addData('2', "rdf", "<rdf/>")

    def testGetMultipleData(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData('id:1', "oai_dc", "<one/>")
        s.addData('id:2', "oai_dc", "<two/>")
        s.addData('id:3', "oai_dc", "<three/>")
        s.addData('id:4', "oai_dc", "<four/>")
        result = list(s.getMultipleData("oai_dc", ['id:2', 'id:3']))
        self.assertEquals([('id:2', "<two/>"), ('id:3', "<three/>")], result)

    def testGetMultipleDataResultNotFound(self):
        s = MultiSequentialStorage(self.tempdir)
        try:
            list(s.getMultipleData("na", ['42']))
            self.fail()
        except KeyError, e:
            self.assertEquals("'na'", str(e))
        s.addData(identifier='1', name='na', data='ignored')
        try:
            list(s.getMultipleData("na", ['42']))
            self.fail()
        except KeyError, e:
            self.assertEquals("'42'", str(e))

    def testGetMultipleDataIgnoreMissingKeysWithFlag(self):
        s = MultiSequentialStorage(self.tempdir)
        result = list(s.getMultipleData(name='sub', identifiers=('1', '42'), ignoreMissing=True))
        self.assertEquals([], result)

        s.addData(identifier='1', name="sub", data="d1")
        s.addData(identifier='2', name="sub", data="d2")
        s.addData(identifier='3', name="sub", data="d3")
        result = list(s.getMultipleData(name="sub", identifiers=('1', '42'), ignoreMissing=True))
        self.assertEquals([('1', "d1")], result)

    def testPartNameEscaping(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData(identifier='2', name="ma/am", data="data")
        s.close()
        s = MultiSequentialStorage(self.tempdir)
        self.assertEquals("data", s.getData('2', "ma/am"))

    def testDirectoryCreatedIfNotExists(self):
        MultiSequentialStorage(join(self.tempdir, "storage"))
        self.assertTrue(isdir(join(self.tempdir, "storage")))

    def testDeleteDataForPart(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData('2', "part1", "data1")
        s.addData('2', "part2", "data2")
        s.deleteData('2', 'part1')
        self.assertRaises(KeyError, lambda: s.getData('2', 'part1'))
        self.assertEquals('data2', s.getData('2', 'part2'))

    def testDeleteDataForAllParts(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData('2', "part1", "data1")
        s.addData('2', "part2", "data2")
        s.deleteData('2')
        self.assertRaises(KeyError, lambda: s.getData('2', 'part1'))
        self.assertRaises(KeyError, lambda: s.getData('2', 'part2'))

    def testCommit(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData('2', "part1", "data1")
        self.assertEquals({'2': 'data1'}, s._storage['part1']._latestModifications)
        s.commit()
        self.assertEquals({}, s._storage['part1']._latestModifications)
        self.assertEquals('data1', s.getData('2', 'part1'))
