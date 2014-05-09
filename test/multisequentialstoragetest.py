from seecr.test import SeecrTestCase

from os.path import isfile, join, isdir

from weightless.core import consume

from meresco.sequentialstore import MultiSequentialStorage


class MultiSequentialStorageTest(SeecrTestCase):
    def testWriteFilePerPart(self):
        s = MultiSequentialStorage(self.tempdir)
        consume(s.add(1, "oai_dc", "<data/>"))
        consume(s.add(2, "rdf", "<rdf/>"))
        self.assertTrue(isfile(join(self.tempdir, "oai_dc")))
        self.assertTrue(isfile(join(self.tempdir, "rdf")))

    def testGetForUnknownPart(self):
        s = MultiSequentialStorage(self.tempdir)
        self.assertRaises(IndexError, lambda: s.getData(42, 'oai_dc'))

    def testGetForUnknownIdentifier(self):
        s = MultiSequentialStorage(self.tempdir)
        consume(s.add(1, "oai_dc", "x"))
        self.assertRaises(IndexError, lambda: s.getData(42, 'oai_dc'))

    def testReadWriteData(self):
        s = MultiSequentialStorage(self.tempdir)
        consume(s.add(1, "oai_dc", "<data/>"))
        s.flush()
        sReopened = MultiSequentialStorage(self.tempdir)
        self.assertEquals('<data/>', sReopened.getData(1, 'oai_dc'))

    def testReadWriteIdentifier(self):
        s = MultiSequentialStorage(self.tempdir)
        consume(s.add(1, "oai_dc", "<data>1</data>"))
        consume(s.add(2, "oai_dc", "<data>2</data>"))
        s.flush()
        sReopened = MultiSequentialStorage(self.tempdir)
        self.assertEquals('<data>1</data>', sReopened.getData(1, 'oai_dc'))
        self.assertEquals('<data>2</data>', sReopened.getData(2, 'oai_dc'))

    def testMonotonicityNotRequiredOverDifferentParts(self):
        s = MultiSequentialStorage(self.tempdir)
        consume(s.add(2, "oai_dc", "<data/>"))
        consume(s.add(2, "rdf", "<rdf/>"))

    def testIterData(self):
        s = MultiSequentialStorage(self.tempdir)
        consume(s.add(2, "oai_dc", "<two/>"))
        consume(s.add(4, "oai_dc", "<four/>"))
        consume(s.add(7, "oai_dc", "<seven/>"))
        self.assertEquals([(2, '<two/>'), (4, '<four/>')], list(s.iterData("oai_dc", 1, 5)))
        self.assertEquals([(7, '<seven/>')], list(s.iterData("oai_dc", 5, 9)))
        self.assertEquals("<two/>", s.getData(2, "oai_dc"))

    def testIterDataUntil(self):
        s = MultiSequentialStorage(self.tempdir)
        s.addData(name='oai_dc', key=2, data="two")
        s.addData(name='oai_dc', key=4, data="four")
        s.addData(name='oai_dc', key=6, data="six")
        s.addData(name='oai_dc', key=7, data="seven")
        s.addData(name='oai_dc', key=8, data="eight")
        s.addData(name='oai_dc', key=9, data="nine")
        i = s.iterData(name='oai_dc', start=0, stop=5)
        self.assertEquals([(2, "two"), (4, "four")], list(i))
        i = s.iterData(name='oai_dc', start=4, stop=7)
        self.assertEquals([(4, "four"), (6, "six")], list(i))
        i = s.iterData(name='oai_dc', start=4, stop=7, inclusive=True)
        self.assertEquals([(4, "four"), (6, "six"), (7, 'seven')], list(i))
        i = s.iterData(name='oai_dc', start=5, stop=99)
        self.assertEquals([(6, "six"), (7, "seven"), (8, "eight"), (9, "nine")], list(i))

    def testGetMultipleData(self):
        s = MultiSequentialStorage(self.tempdir)
        consume(s.add(1, "oai_dc", "<one/>"))
        consume(s.add(2, "oai_dc", "<two/>"))
        consume(s.add(3, "oai_dc", "<three/>"))
        consume(s.add(4, "oai_dc", "<four/>"))
        result = list(s.getMultipleData("oai_dc", [2, 3]))
        self.assertEquals([(2, "<two/>"), (3, "<three/>")], result)

    def testGetMultipleDataResultNotFound(self):
        s = MultiSequentialStorage(self.tempdir)
        try:
            list(s.getMultipleData("na", [42]))
            self.fail()
        except KeyError, e:
            self.assertEquals('42', str(e))

    def testGetMultipleDataIgnoreMissingKeysWithFlag(self):
        s = MultiSequentialStorage(self.tempdir)
        result = list(s.getMultipleData(name='sub', keys=(1, 42), ignoreMissing=True))
        self.assertEquals([], result)

        s.addData(key=1, name="sub", data="d1")
        s.addData(key=2, name="sub", data="d2")
        s.addData(key=3, name="sub", data="d3")
        result = list(s.getMultipleData(name="sub", keys=(1, 42), ignoreMissing=True))
        self.assertEquals([(1, "d1")], result)

    def testPartNameEscaping(self):
        s = MultiSequentialStorage(self.tempdir)
        consume(s.add(2, "ma/am", "data"))
        s.flush()
        s = MultiSequentialStorage(self.tempdir)
        self.assertEquals("data", s.getData(2, "ma/am"))

    def testDirectoryCreatedIfNotExists(self):
        MultiSequentialStorage(join(self.tempdir, "storage"))
        self.assertTrue(isdir(join(self.tempdir, "storage")))
