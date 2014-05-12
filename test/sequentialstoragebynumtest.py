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

from os import fstat, SEEK_CUR
from os.path import join, dirname, abspath
from random import random, randint
from time import time
from itertools import islice

from seecr.test import SeecrTestCase
from weightless.core import consume

from meresco.sequentialstore._sequentialstoragebynum import _SequentialStorageByNum, SENTINEL, DEFAULT_BLOCK_SIZE


testDataDir = join(dirname(__file__), 'data')

# Ideas:
# - Galloping iso bisect for getMultiple
# - Adjectenly entering new blk -> scan i.s.o. _readNext(), not needed - optimize?

class SequentialStorageByNumTest(SeecrTestCase):
    def testSentinalWritten(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(3, "data")
        s.flush()
        self.assertEquals("----\n3\n12\nx\x9cKI,I\x04\x00\x04\x00\x01\x9b\n",
                open(self.tempfile).read())

    def testGetForUnknownIdentifier(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(1, 'x')
        self.assertRaises(IndexError, lambda: s[42])

    def testReadWriteKey(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(1, "<data>1</data>")
        s.add(2, "<data>2</data>")
        s.flush()
        sReopened = _SequentialStorageByNum(self.tempfile)
        self.assertEquals('<data>1</data>', sReopened[1])
        self.assertEquals('<data>2</data>', sReopened[2])

    def testKeyIsMonotonicallyIncreasing(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(3, "na")
        s.add(4, "na")
        try:
            consume(s.add(2, "na"))
            self.fail()
        except ValueError, e:
            self.assertEquals("key 2 must be greater than last key 4", str(e))

    def testNumbersAsStringIsProhibited(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(2, "na")
        s.add(10, "na")
        self.assertRaises(ValueError, lambda: s.add('3', "na"))

    def testKeyIsMonotonicallyIncreasingAfterReload(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(3, "na")
        s.flush()
        s = _SequentialStorageByNum(self.tempfile)
        self.assertRaises(ValueError, lambda: consume(s.add(2, "na")))

    def testDataCanBeEmptyButStoredItemIsNeverShorterThanBlocksize(self):
        blksiz = 11
        s = _SequentialStorageByNum(self.tempfile, blockSize=11)
        s.add(0, '')
        s.flush()
        fileData = open(self.tempfile, 'rb').read()
        self.assertTrue(len(fileData) >= blksiz, len(fileData))

        # whitebox, blocksize 'mocked data' is 1-byte
        from zlib import compress
        self.assertEquals(18, len(fileData))
        self.assertEquals(blksiz - 1 + len(compress('')), len(fileData))

    def testLastKeyFoundInCaseOfLargeBlock(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(1, 'record 1')
        s.add(2, randomString(8 * DEFAULT_BLOCK_SIZE))  # compressed multiple times BLOCKSIZE
        s.flush()
        s = _SequentialStorageByNum(self.tempfile)
        self.assertEquals(2, s.lastKey)

    def testGetMultiple(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(1, "<one/>")
        s.add(2, "<two/>")
        s.add(3, "<three/>")
        s.add(4, "<four/>")
        result = list(s.getMultiple([2, 3]))
        self.assertEquals([(2, "<two/>"), (3, "<three/>")], result)

    def testGetMultipleNoResults(self):
        s = _SequentialStorageByNum(self.tempfile)
        result = list(s.getMultiple([]))
        self.assertEquals([], result)

    def testGetMultipleResultNotFound(self):
        s = _SequentialStorageByNum(self.tempfile)
        try:
            list(s.getMultiple([42]))
            self.fail()
        except KeyError, e:
            self.assertEquals('42', str(e))

    def testGetMultipleResultNotFound2(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(1, "<one/>")
        results = s.getMultiple([1, 2])
        key, data = results.next()
        self.assertEquals((1, "<one/>"), (key, data))
        try:
            results.next()
            self.fail()
        except KeyError, e:
            self.assertEquals('2', str(e))

    def testGetMultipleWithKeysInOneBlock(self):
        s = _SequentialStorageByNum(self.tempfile, blockSize=102400)
        s.add(1, "d1")
        s.add(2, "d2")
        s.add(3, "d3")
        result = list(s.getMultiple(keys=(1, 3)))
        self.assertEquals([(1, "d1"), (3, "d3")], result)

    def testGetMultipleIgnoresMissingKeysWithFlag(self):
        s = _SequentialStorageByNum(self.tempfile)
        result = list(s.getMultiple(keys=(1, 42), ignoreMissing=True))
        self.assertEquals([], result)

        s.add(key=1, data="d1")
        s.add(key=2, data="d2")
        s.add(key=3, data="d3")
        result = list(s.getMultiple(keys=(1, 42), ignoreMissing=True))
        self.assertEquals([(1, "d1")], result)

    def testGetMultipleWithKeysAcrossMultipleBlocks(self):
        class File(object):
            def __init__(me):
                me._f = open(self.tempfile, "ab+")
                me.offsets = []
            def __getattr__(me, name):
                return getattr(me._f, name)
            def seek(me, offset):
                me.offsets.append(offset)
                return me._f.seek(offset)
        f = File()
        s = _SequentialStorageByNum("na", file_=f, blockSize=100)
        offsets = []
        one = randomString(50)
        three = randomString(50)
        four = randomString(50)
        s.add(1, one )
        offsets.append(f.tell())
        s.add(2, randomString(50))
        offsets.append(f.tell())
        s.add(3, three)
        offsets.append(f.tell())
        s.add(4, four)
        offsets.append(f.tell())
        list(s.getMultiple(keys=(1, 3, 4)))
        self.assertEquals({0: 1, 1: 3, 2: 4}, s._blkIndex._cache)
        f.offsets[:] = []
        result = list(s.getMultiple(keys=(1, 3, 4)))
        self.assertEquals([(1, one), (3, three), (4, four)], result)
        self.assertEquals([0, offsets[0], offsets[2]], f.offsets)

    def testTwoAlternatingGetMultipleIterators(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(2, "<data>two</data>")
        s.add(4, "<data>four</data>")
        s.add(7, "<data>seven</data>")
        s.add(9, "<data>nine</data>")
        i1 = s.getMultiple((4, 9))
        i2 = s.getMultiple((2, 7))
        self.assertEquals((4, "<data>four</data>"), i1.next())
        self.assertEquals((2, "<data>two</data>"), i2.next())
        self.assertEquals((9, "<data>nine</data>"), i1.next())
        self.assertEquals((7, "<data>seven</data>"), i2.next())

    def testKeysMustBeSortedForGetMultiple(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(1, 'x')
        s.add(2, '_')
        s.add(3, 'z')
        result = s.getMultiple(keys=[1, 3, 2])

        self.assertEquals((1, 'x'), result.next())
        self.assertEquals((3, 'z'), result.next())
        try:
            result.next()
        except ValueError, e:
            self.assertEquals('Keys should have been sorted.', str(e))
        else: self.fail()

        result = s.getMultiple(keys=[3, 3])
        self.assertEquals((3, 'z'), result.next())
        self.assertRaises(ValueError, lambda: result.next())

    def testKeysMustBeIntsForGetMultiple(self):
        s = _SequentialStorageByNum(self.tempfile)
        result = s.getMultiple(keys=['1'])
        try:
            result.next()
        except ValueError, e:
            self.assertEquals('Expected int', str(e))
        else: self.fail()

    def testGetItem(self):
        # getitem need not be completely correct for bisect to work
        # the functionality below is good enough I suppose.
        # As a side effect, it solves back scanning! We let
        # bisect do that for us.
        s = _SequentialStorageByNum(self.tempfile, blockSize=11)
        self.assertEquals(0, len(s._blkIndex))
        s.add(2, "<data>two is nice</data>")
        s.add(4, "<data>four goes fine</data>")
        s.add(7, "<data>seven seems ok</data>")
        self.assertEquals(12, len(s._blkIndex))
        self.assertEquals((2, "<data>two is nice</data>"), s._blkIndex.scan(0))
        self.assertEquals((4, "<data>four goes fine</data>"), s._blkIndex.scan(1))
        self.assertEquals((4, "<data>four goes fine</data>"), s._blkIndex.scan(2))
        self.assertEquals((4, "<data>four goes fine</data>"), s._blkIndex.scan(3))
        self.assertEquals((7, "<data>seven seems ok</data>"), s._blkIndex.scan(4))
        self.assertEquals((7, "<data>seven seems ok</data>"), s._blkIndex.scan(5))
        self.assertEquals((7, "<data>seven seems ok</data>"), s._blkIndex.scan(6))
        self.assertEquals((7, "<data>seven seems ok</data>"), s._blkIndex.scan(7))
        # hmm, we expect index 0-10 to work based on len()
        self.assertRaises(StopIteration, lambda: s._blkIndex.scan(8))

    def testIndexItem(self):
        s = _SequentialStorageByNum(self.tempfile, blockSize=11)
        self.assertEquals(0, len(s._blkIndex))
        s.add(2, "<data>two</data>")
        s.add(4, "<data>four</data>")
        s.add(7, "<data>seven</data>")
        self.assertEquals(9, len(s._blkIndex))
        self.assertEquals("<data>four</data>", s[4])
        self.assertEquals("<data>two</data>", s[2])
        self.assertEquals("<data>seven</data>", s[7])

    def testIndexNotFound(self):
        s = _SequentialStorageByNum(self.tempfile)
        self.assertRaises(IndexError, lambda: s[2])
        s.add(2, "<data>two</data>")
        self.assertRaises(IndexError, lambda: s[1])
        self.assertRaises(IndexError, lambda: s[3])
        s.add(4, "<data>four</data>")
        self.assertRaises(IndexError, lambda: s[1])
        self.assertRaises(IndexError, lambda: s[3])
        self.assertRaises(IndexError, lambda: s[5])

    def testIndexWithVerySmallAndVEryLargeRecord(self):
        s = _SequentialStorageByNum(self.tempfile, blockSize=11)
        self.assertEquals(0, len(s._blkIndex))
        s.add(2, "<data>short</data>")
        s.add(4, ''.join("<%s>" % i for i in xrange(10000)))
        self.assertEquals(2012, len(s._blkIndex))
        self.assertEquals("<data>short</data>", s[2])
        self.assertEquals("<0><1><2><3><4><5><6", s[4][:20])

    def testNewLineInData(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(4, "here follows\na new line")
        self.assertEquals("here follows\na new line", s[4])

    def testSentinelInData(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(2, "<data>two</data>")
        s.add(5, ("abc%sxyz" % (SENTINEL+'\n')) * 10)
        s.add(7, "<data>seven</data>")
        s.add(9, "<data>nine</data>")
        self.assertEquals("abc----\nxyzabc----\nx", s[5][:20])
        self.assertEquals("<data>seven</data>", s[7])

    def testReadNextWithTargetKey(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(3, "three")
        s.add(4, "four")
        s.add(7, "seven")
        s.add(9, "nine")
        s._f.seek(0)
        self.assertRaises(StopIteration, lambda: s._readNext(target_key=2))
        self.assertRaises(StopIteration, lambda: s._readNext(target_key=5))
        s._f.seek(0)
        self.assertEquals("three", s._readNext(target_key=3)[1])
        self.assertEquals("four", s._readNext(target_key=4)[1])
        s._f.seek(0)
        self.assertEquals("four", s._readNext(target_key=4)[1])
        self.assertEquals("seven", s._readNext(target_key=7)[1])
        s._f.seek(0)
        self.assertEquals("nine", s._readNext(target_key=9)[1])
        try:
            s._readNext(target_key=3)
            self.fail()
        except StopIteration:
            pass

    def testTargetKeyGreaterOrEquals(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(3, "three")
        s.add(7, "seven")
        s.add(9, "nine")
        s._f.seek(0)
        self.assertEquals("three", s._readNext(target_key=2, greater=True)[1])
        self.assertEquals("seven", s._readNext(target_key=7, greater=True)[1])
        self.assertEquals("nine", s._readNext(target_key=8, greater=True)[1])

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
        s = _SequentialStorageByNum(self.tempfile)
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
        s = _SequentialStorageByNum(self.tempfile)
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
        s = _SequentialStorageByNum(self.tempfile)
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

    def testBlockIndexHasAtLeastOneBlock(self):
        s = _SequentialStorageByNum(self.tempfile)
        self.assertEquals(0, len(s._blkIndex))
        s.add(key=2, data="two")
        self.assertEquals(1, len(s._blkIndex))

    def XXXtestBigBlock(self):
        path = self.createTestIndex("data/test.ss", count=2**20)
        b = _SequentialStorageByNum(path)._blkIndex
        self.assertEquals(272633, len(b))
        self.assertEquals(0, b[0])
        self.assertEquals(4, b[1])
        k = b[783]
        self.assertTrue(k < b[784] < k + 10)
        self.assertEquals(k, b.scan(783, target_key=k)[0])
        self.assertRaises(StopIteration, lambda: b.scan(784, target_key=k)[0])
        self.assertEquals(k+4, b.scan(784, target_key=k+4)[0])
        self.assertEquals(1048575, b[len(b)-1])
        blk = b.search(864123)
        self.assertEquals(224666, blk)
        self.assertEquals(864120, b[blk])
        self.assertEquals(864124, b[blk+1])
        self.assertEquals(864123, b.scan(blk, target_key=864123)[0])

    def createTestIndex(self, path, count=2**20):
        path = ".".join((path, str(count)))
        s = _SequentialStorageByNum(path)
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

    def XXXtestReadSpeed(self):
        count = 2**20
        path = self.createTestIndex("data/test.ss", count=count)
        def readOne(s, count):
            s[randint(0, count-1)]
        def readBatch(s, count, size=100):
            for _ in islice(s.range(randint(0, count-1)), size):
                pass
        def read(blksiz=2**13, fread=readOne):
            s = _SequentialStorageByNum(path, blockSize=blksiz)
            t0 = time()
            for i in xrange(1, 50001):
                fread(s, count)
                if i % 10000 == 0:
                    t1 = time()
                    print "reads:", i, " reads/s:", int(i/(t1-t0)), " cache:", len(s._blkIndex._cache)
        print " ++++ RANDOM ++++ "
        for blocksize in [2**n for n in range(14,15)]:
            print "==== Blksize:", blocksize, "===="
            read(blocksize)
        print " ++++ SEQUENTIAL ++++ "
        for blocksize in [2**n for n in range(14,15)]:
            print "==== Blksize:", blocksize, "===="
            read(blocksize, readBatch)

    def testShortRubbishAtStartOfFileIgnored(self):
        s = ReopeningSeqStorage(self).write('corrupt')
        self.assertEquals([], s.items())
        s.add(1, "record 1")
        self.assertEquals([(1, 'record 1')], s.items())
        self.assertTrue(s.fileData.startswith("corrupt" + SENTINEL + '\n1\n'), s.fileData)
        self.assertEquals(1, s.lastKey)

    def testCorruptionFromKeyLineIgnored(self):
        s = ReopeningSeqStorage(self).write('%s\ncorrupt' % SENTINEL)
        self.assertEquals([], s.items())
        s.add(1, "record 1")
        self.assertEquals([(1, 'record 1')], s.items())
        self.assertTrue(s.fileData.startswith(SENTINEL + "\ncorrupt" + SENTINEL + '\n1\n'), s.fileData)
        self.assertEquals(1, s.lastKey)

    def testCorruptionFromLengthLineIgnored(self):
        s = ReopeningSeqStorage(self).write('%s\n1\ncorrupt' % SENTINEL)
        self.assertEquals([], s.items())
        s.add(1, "record 1")
        self.assertEquals([(1, 'record 1')], s.items())
        self.assertTrue(s.fileData.startswith(SENTINEL + "\n1\ncorrupt" + SENTINEL + '\n1\n'), s.fileData)
        self.assertEquals(1, s.lastKey)

    def testCorruptionFromDataIgnored(self):
        s = ReopeningSeqStorage(self).write('%s\n1\n100\ncorrupt' % SENTINEL)
        self.assertEquals([], s.items())
        s.add(1, "record 1")
        self.assertEquals([(1, 'record 1')], s.items())
        self.assertTrue(s.fileData.startswith(SENTINEL + "\n1\n100\ncorrupt" + SENTINEL + '\n1\n'), s.fileData)
        self.assertEquals(1, s.lastKey)

    def testRubbishInBetween(self):
        s = ReopeningSeqStorage(self)
        s.add(1, "record 1")
        s.write("rubbish")
        s.add(2, "record 2")
        self.assertEquals([(1, 'record 1'), (2, 'record 2')], s.items())
        self.assertEquals(2, s.lastKey)

    def testCorruptionInBetween(self):
        s = ReopeningSeqStorage(self)
        s.add(5, "record to be corrupted")
        corruptRecordTemplate = s.fileData

        def _writeRecordAndPartOfRecord(i):
            open(self.tempfile, 'w').truncate(0)
            s.add(1, "record 1")
            s.write(corruptRecordTemplate[:i+1])

        for i in xrange(len(corruptRecordTemplate) - 2):
            _writeRecordAndPartOfRecord(i)
            self.assertEquals(1, s.lastKey, "[%s] %s" % (i, s.lastKey))
            s.add(2, "record 2")
            self.assertEquals([1, 2], s.keys())

        for i in xrange(len(corruptRecordTemplate) - 2, len(corruptRecordTemplate)):
            _writeRecordAndPartOfRecord(i)
            self.assertEquals(5, s.lastKey, "[%s] %s" % (i, s.lastKey))
            self.assertEquals([1, 5], s.keys())

    def testLastKeyFoundInCaseOfBlockSizedGapBeforeCorruptRecord(self):
        s = ReopeningSeqStorage(self)
        s.add(1, "record")
        s.write(randomString(4 * DEFAULT_BLOCK_SIZE))
        s.write(SENTINEL + '\n2\n42\n')  # sentinal + key and length, but no data
        # self.assertEquals(1, s.lastKey)  # preferred functionality, but too costly to get right
        try:
            lastKey = s.lastKey
        except AssertionError, e:
            self.assertEquals('SequentialStorage in %s internally inconsistent.' % abspath(self.tempfile), str(e))
        else:
            self.fail('unexpectedly found lastKey: %s' % lastKey)

    def testTargetKeySkipsRubbish(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(5, "five")
        s._f.write("@!^$#%")
        s.add(6, "six")
        s._f.seek(0)
        self.assertEquals("six", s._readNext(target_key=6)[1])

    def testTargetKeyDoesNotSkipRecordWhenRubbishPresent(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(5, "five")
        s._f.seek(-2, SEEK_CUR)
        s._f.write("@!^$#%")
        s.add(6, "six")
        s._f.seek(0)
        self.assertEquals("six", s._readNext(target_key=6)[1])

    def testTargetKeyDoesNotSkipRecordThatHappensToBeTruncated(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(5, "five")
        s._f.truncate(s._f.tell()-2)  # 5 becomes crap, but six is still ok
        s.add(6, "six")
        s._f.seek(0)
        try:
            s._readNext(target_key=5)
            self.fail()
        except StopIteration:
            pass
        s._f.seek(0) #TODO what if this seek is not there? What should the file position be?
        self.assertEquals("six", s._readNext(target_key=6)[1])

    def testClose(self):
        s = _SequentialStorageByNum(self.tempfile)
        s.add(1, "one")
        s.close()
        self.assertEquals('----\n1\n11\nx\x9c\xcb\xcfK\x05\x00\x02\x91\x01C\n', open(join(self.tempfile)).read())
        self.assertRaises(ValueError, lambda: s.add(2, 'two'))


class ReopeningSeqStorage(object):
    def __init__(self, testCase):
        self.tempfile = testCase.tempfile

    def add(self, key, data):
        s = self.seqStorage()
        s.add(key, data)
        s.flush()
        return self

    def keys(self):
        return [item[0] for item in self.items()]

    def items(self):
        s = self.seqStorage()
        return list(s.range(0))

    def write(self, rubbish):
        with open(self.tempfile, 'ab') as f:
            f.write(rubbish)
        return self

    @property
    def fileData(self):
        return open(self.tempfile).read()

    @property
    def lastKey(self):
        s = self.seqStorage()
        return s.lastKey

    def seqStorage(self):
        return _SequentialStorageByNum(self.tempfile)




def randomString(n):
    return ''.join(chr(randint(0, 255)) for x in xrange(n))
