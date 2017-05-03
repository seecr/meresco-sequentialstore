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

from seecr.test import SeecrTestCase
from testutils import randomString

from os import fstat, SEEK_CUR, stat
from os.path import join, dirname, abspath
from random import random
from time import time
from itertools import islice

from meresco.sequentialstore.seqstorestore import SeqStoreStore


testDataDir = join(dirname(__file__), 'data')

class SequentialStorageByNumTest(SeecrTestCase):

    def testGetForUnknownIdentifier(self):
        s = SeqStoreStore(self.tempdir)
        s.add(1, 'x')
        self.assertRaises(Exception, lambda: s.get(42))

    def testReadWriteKey(self):
        s = SeqStoreStore(self.tempdir)
        s.add(1, "<data>1</data>")
        s.add(2, "<data>2</data>")
        s.close()
        sReopened = SeqStoreStore(self.tempdir)
        self.assertEquals('<data>1</data>', sReopened.get(1))
        self.assertEquals('<data>2</data>', sReopened.get(2))

    def testKeyIsMonotonicallyIncreasing(self):
        s = SeqStoreStore(self.tempdir)
        s.add(3, "na")
        s.add(4, "na")
        try:
            s.add(2, "na")
            self.fail()
        except ValueError, e:
            self.assertEquals("key 2 must be greater than last key 4", str(e))

    def testNumbersAsStringIsProhibited(self):
        s = SeqStoreStore(self.tempdir)
        s.add(2, "na")
        s.add(10, "na")
        self.assertRaises(ValueError, lambda: s.add('3', "na"))

    def testKeyIsMonotonicallyIncreasingAfterReload(self):
        s = SeqStoreStore(self.tempdir)
        s.add(3, "na")
        s.close()
        s = SeqStoreStore(self.tempdir)
        self.assertRaises(ValueError, lambda: s.add(2, "na"))

    def testGetMultiple(self):
        s = SeqStoreStore(self.tempdir)
        s.add(1, "<one/>")
        s.add(2, "<two/>")
        s.add(3, "<three/>")
        s.add(4, "<four/>")
        result = list(s.getMultiple([2, 3], False))
        self.assertEquals([(2, "<two/>"), (3, "<three/>")], result)

    def testGetMultipleNoResults(self):
        s = SeqStoreStore(self.tempdir)
        result = list(s.getMultiple([], False))
        self.assertEquals([], result)

    def testGetMultipleResultNotFound(self):
        s = SeqStoreStore(self.tempdir)
        try:
            list(s.getMultiple([42], False))
            self.fail()
        except KeyError, e:
            self.assertEquals('42', str(e))

    def testGetMultipleResultNotFound2(self):
        s = SeqStoreStore(self.tempdir)
        s.add(1, "<one/>")
        results = s.getMultiple([1, 2], False)
        key, data = results.next()
        self.assertEquals((1, "<one/>"), (key, data))
        try:
            results.next()
            self.fail()
        except KeyError, e:
            self.assertEquals('2', str(e))

    def testGetMultipleWithKeysInOneBlock(self):
        s = SeqStoreStore(self.tempdir)
        s.add(1, "d1")
        s.add(2, "d2")
        s.add(3, "d3")
        result = list(s.getMultiple((1, 3), False))
        self.assertEquals([(1, "d1"), (3, "d3")], result)

    def testGetMultipleIgnoresMissingKeysWithFlag(self):
        s = SeqStoreStore(self.tempdir)
        result = list(s.getMultiple((1, 42), True))
        self.assertEquals([], result)

        s.add(key=1, data="d1")
        s.add(key=2, data="d2")
        s.add(key=3, data="d3")
        result = list(s.getMultiple((1, 42), True))
        self.assertEquals([(1, "d1")], result)

    def testTwoAlternatingGetMultipleIterators(self):
        s = SeqStoreStore(self.tempdir)
        s.add(2, "<data>two</data>")
        s.add(4, "<data>four</data>")
        s.add(7, "<data>seven</data>")
        s.add(9, "<data>nine</data>")
        i1 = s.getMultiple((4, 9), False)
        i2 = s.getMultiple((2, 7), False)
        self.assertEquals((4, "<data>four</data>"), i1.next())
        self.assertEquals((2, "<data>two</data>"), i2.next())
        self.assertEquals((9, "<data>nine</data>"), i1.next())
        self.assertEquals((7, "<data>seven</data>"), i2.next())

    def testKeysMustBeSortedForGetMultiple(self):
        s = SeqStoreStore(self.tempdir)
        s.add(1, 'x')
        s.add(2, '_')
        s.add(3, 'z')
        result = s.getMultiple([1, 3, 2], False)

        self.assertEquals((1, 'x'), result.next())
        self.assertEquals((3, 'z'), result.next())
        try:
            result.next()
        except ValueError, e:
            self.assertEquals('Keys should have been sorted.', str(e))
        else: self.fail()

        result = s.getMultiple([3, 3], False)
        self.assertEquals((3, 'z'), result.next())
        self.assertRaises(ValueError, lambda: result.next())

    def testKeysMustBeIntsForGetMultiple(self):
        s = SeqStoreStore(self.tempdir)
        result = s.getMultiple(['1'], False)
        try:
            result.next()
        except ValueError, e:
            self.assertEquals('Expected int', str(e))
        else: self.fail()

    def testIndexNotFound(self):
        s = SeqStoreStore(self.tempdir)
        self.assertRaises(IndexError, lambda: s[2])
        s.add(2, "<data>two</data>")
        self.assertRaises(IndexError, lambda: s[1])
        self.assertRaises(IndexError, lambda: s[3])
        s.add(4, "<data>four</data>")
        self.assertRaises(IndexError, lambda: s[1])
        self.assertRaises(IndexError, lambda: s[3])
        self.assertRaises(IndexError, lambda: s[5])

    def testIndexWithVerySmallAndVEryLargeRecord(self):
        s = SeqStoreStore(self.tempdir)
        s.add(2, "<data>short</data>")
        s.add(4, ''.join("<%s>" % i for i in xrange(10000)))
        self.assertEquals("<data>short</data>", s[2])
        self.assertEquals("<0><1><2><3><4><5><6", s[4][:20])

    def testNewLineInData(self):
        s = SeqStoreStore(self.tempdir)
        s.add(4, "here follows\na new line")
        self.assertEquals("here follows\na new line", s[4])

    def testCompression(self):
        import zlib, bz2
        def ratio(filename, compress):
            data = open(filename).read()
            compressed = compress(data)
            return len(data)/float(len(compressed))
        trijntjeGgc = join(testDataDir, 'trijntje.ggc.xml')
        trijntjeXml = join(testDataDir, 'trijntje.xml')

        zlib_ratio = ratio(trijntjeGgc, zlib.compress)
        bz2_ratio = ratio(trijntjeGgc, bz2.compress)
        self.assertTrue(3.0 < bz2_ratio < 3.1, bz2_ratio)
        self.assertTrue(3.4 < zlib_ratio < 3.5, zlib_ratio)
        zlib_ratio = ratio(trijntjeXml, zlib.compress)
        bz2_ratio = ratio(trijntjeXml, bz2.compress)
        self.assertTrue(2.5 < bz2_ratio < 2.6, bz2_ratio)
        self.assertTrue(3.2 < zlib_ratio < 3.3, zlib_ratio)

    def testRange(self):
        s = SeqStoreStore(self.tempdir)
        s.add(2, "<data>two</data>")
        s.add(4, "<data>four</data>")
        s.add(7, "<data>seven</data>")
        s.add(9, "<data>nine</data>")
        i = s.range(3)
        self.assertEquals((4, "<data>four</data>"), i.next())
        self.assertEquals((7, "<data>seven</data>"), i.next())
        self.assertEquals((9, "<data>nine</data>"), i.next())
        self.assertRaises(StopIteration, lambda: i.next())

    def testTwoAlternatingRangeIterators(self):
        s = SeqStoreStore(self.tempdir)
        s.add(2, "<data>two</data>")
        s.add(4, "<data>four</data>")
        s.add(7, "<data>seven</data>")
        s.add(9, "<data>nine</data>")
        i1 = s.range(4)
        i2 = s.range(7)
        self.assertEquals((7, "<data>seven</data>"), i2.next())
        self.assertEquals((4, "<data>four</data>"), i1.next())
        self.assertEquals((9, "<data>nine</data>"), i2.next())
        self.assertEquals((7, "<data>seven</data>"), i1.next())
        self.assertRaises(StopIteration, lambda: i2.next())
        self.assertEquals((9, "<data>nine</data>"), i1.next())
        self.assertRaises(StopIteration, lambda: i1.next())

    def testRangeUntil(self):
        s = SeqStoreStore(self.tempdir)
        s.add(2, "two")
        s.add(4, "four")
        s.add(6, "six")
        s.add(7, "seven")
        s.add(8, "eight")
        s.add(9, "nine")
        i = s.range(0, 5)
        self.assertEquals([(2, "two"), (4, "four")], list(i))
        i = s.range(4, 7)
        self.assertEquals([(4, "four"), (6, "six")], list(i))
        i = s.range(4, 7, inclusive=True)
        self.assertEquals([(4, "four"), (6, "six"), (7, 'seven')], list(i))
        i = s.range(5, 99)
        self.assertEquals([(6, "six"), (7, "seven"), (8, "eight"), (9, "nine")], list(i))

    def createTestIndex(self, path, count=2**20):
        path = ".".join((path, str(count)))
        s = SeqStoreStore(path)
        t0 = time()
        if fstat(s._f.fileno()).st_size == 0:
            print "Creating test store in:", repr(path), "of", count, "items."
            data = ''.join(str(random()) for f in xrange(300))
            for i in xrange(count):
                s.add(i, data)
                if i % 10000 == 0:
                    t1 = time()
                    print "writes:", i, " writes/s: ", int(i / (t1-t0))
                    from sys import stdout; stdout.flush()
        del s
        return path
