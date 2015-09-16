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

from seecr.test import SeecrTestCase, CallTrace

from os.path import join, isfile
from random import shuffle

from meresco.sequentialstore import SequentialStorage
from meresco.sequentialstore.sequentialstorage import _Index
from subprocess import Popen, PIPE


class SequentialStorageTest(SeecrTestCase):
    def testAddGetItem(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        self.assertEquals("1", sequentialStorage['abc'])

    def testKeyErrorForUnknownKey(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        self.assertRaises(KeyError, lambda: sequentialStorage['abc'])

    def testPersisted(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        sequentialStorage.add(identifier='def', data="2")
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        self.assertEquals("1", sequentialStorageReloaded['abc'])
        self.assertEquals("2", sequentialStorageReloaded['def'])

    def testGetMultiple(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        sequentialStorage.add(identifier='def', data="2")
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        self.assertEquals([('abc', '1'), ('def', '2')], list(sequentialStorageReloaded.getMultiple(identifiers=['abc', 'def'])))

    def testGetMultipleCoercesToStr(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='1', data="x")
        sequentialStorage.add(identifier='2', data="y")
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        self.assertEquals([('1', 'x'), ('2', 'y')], list(sequentialStorageReloaded.getMultiple(identifiers=[1, 2])))

    def testGetMultipleDifferentOrder(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='def', data="1")
        sequentialStorage.add(identifier='abc', data="2")
        self.assertEquals([('def', '1'), ('abc', '2')], list(sequentialStorage.getMultiple(identifiers=['abc', 'def'])))

    def testMultipleIgnoreMissing(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        self.assertRaises(KeyError, lambda: list(sequentialStorage.getMultiple(identifiers=['abc', 'def'], ignoreMissing=False)))
        self.assertEquals([('abc', '1')], list(sequentialStorage.getMultiple(identifiers=['abc', 'def'], ignoreMissing=True)))

    def testKeyMonotonicallyIncreasingAfterReopening(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        sequentialStorage.add(identifier='def', data="2")
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        sequentialStorageReloaded.add(identifier='ghi', data="3")

        self.assertEquals("1", sequentialStorageReloaded['abc'])
        self.assertEquals("2", sequentialStorageReloaded['def'])
        self.assertEquals("3", sequentialStorageReloaded['ghi'])

    def testDelete(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        sequentialStorage.add(identifier='def', data="2")
        sequentialStorage.delete(identifier='abc')
        self.assertRaises(KeyError, lambda: sequentialStorage['abc'])
        self.assertEquals('2', sequentialStorage['def'])

    def testDeletePersisted(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        sequentialStorage.add(identifier='def', data="2")
        sequentialStorage.delete(identifier='abc')
        sequentialStorage.close()

        sequentialStorage = SequentialStorage(self.tempdir)
        self.assertRaises(KeyError, lambda: sequentialStorage['abc'])
        self.assertEquals('2', sequentialStorage['def'])

    def testClose(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        self.assertEquals('', open(sequentialStorage._seqStorageByNum._f.name).read())
        lockFile = join(self.tempdir, 'index', 'write.lock')
        self.assertTrue(isfile(lockFile))
        seqStorageFileName = sequentialStorage._seqStorageByNum._f.name
        sequentialStorage.close()
        self.assertEquals('----\n1\n14\nx\x9c\xd3NLJ\xe62\x04\x00\x05\xe3\x01\x8d\n', open(seqStorageFileName).read())
        stdout, stderr = Popen("lsof -n %s" % lockFile, stdout=PIPE, stderr=PIPE, shell=True).communicate()
        self.assertEquals('', stdout.strip())
        self.assertRaises(AttributeError, lambda: sequentialStorage.add('def', data='2'))

    def testGet(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        self.assertEquals(None, sequentialStorage.get('abc'))
        self.assertEquals('x', sequentialStorage.get(identifier='abc', default='x'))
        self.assertEquals('x', sequentialStorage.get('abc', 'x'))

    def testVersionWritten(self):
        SequentialStorage(self.tempdir)
        version = open(join(self.tempdir, "sequentialstorage.version")).read()
        self.assertEquals('2', version)

    def testRefuseInitWithNoVersionFile(self):
        open(join(self.tempdir, 'x'), 'w').close()
        try:
            SequentialStorage(self.tempdir)
            self.fail()
        except AssertionError, e:
            self.assertEquals('The SequentialStorage at %s needs to be converted to the current version.' % self.tempdir, str(e))

    def testRefuseInitWithDifferentVersionFile(self):
        open(join(self.tempdir, 'sequentialstorage.version'), 'w').write('different version')
        try:
            SequentialStorage(self.tempdir)
            self.fail()
        except AssertionError, e:
            self.assertEquals('The SequentialStorage at %s needs to be converted to the current version.' % self.tempdir, str(e))

    def testRefuseInitWithDirectoryPathThatExistsAsFile(self):
        filePath = join(self.tempdir, 'x')
        open(filePath, 'w').close()
        try:
            SequentialStorage(filePath)
            self.fail()
        except AssertionError, e:
            self.assertEquals('Given directory name %s exists as file.' % filePath, str(e))

    def testShouldNotAllowOpeningTwice(self):
        SequentialStorage(self.tempdir)
        try:
            SequentialStorage(self.tempdir)
            self.fail()
        except Exception, e:
            self.assertTrue(repr(e).startswith('JavaError(<Throwable: org.apache.lucene.store.LockObtainFailedException: Lock obtain timed out: NativeFSLock'), e)

    def testIndexIterValues(self):
        index = _Index(self.tempdir)
        index['id0'] = 1
        index['id1'] = 8
        values = list(index.itervalues())
        self.assertEquals([1, 8], values)

    def testIndexLen(self):
        index = _Index(self.tempdir)
        self.assertEquals(0, len(index))

        index['id0'] = 1
        index['id1'] = 2
        self.assertEquals(2, len(index))

        del index['id1']
        self.assertEquals(1, len(index))

        index['id1'] = 20
        del index['id0']
        self.assertEquals(1, len(index))

        del index['id1']
        self.assertEquals(0, len(index))

    def testIndexIterValuesAfterDelete(self):
        index = _Index(self.tempdir)
        index['id0'] = 1
        index['id1'] = 8
        del index['id0']
        values = list(index.itervalues())
        self.assertEquals([8], values)

    def testIndexItervaluesAfterUpdate(self):
        index = _Index(self.tempdir)
        index['id0'] = 1
        self.assertEquals([1], list(index.itervalues()))
        index['id0'] = 2
        self.assertEquals([2], list(index.itervalues()))

    def testIndexItervaluesSorting(self):
        index = _Index(self.tempdir)
        index['id0'] = 3
        index['id1'] = 2
        index['id2'] = 1
        self.assertEquals([1, 2, 3], list(index.itervalues()))
        index['id0'] = 4
        self.assertEquals([1, 2, 4], list(index.itervalues()))

    def testIndexIterMoreThanAFewValues(self):
        index = _Index(self.tempdir)
        for i in xrange(1, 15):
            index['id%s' % i] = i * 7
        values = list(index.itervalues())
        self.assertEquals(14, len(values))

    def testIndexIterManyValuesAfterMerge(self):
        bakje = range(1, 3000)
        shuffle(bakje)
        index = _Index(self.tempdir)
        for i in xrange(2000):
            index['id%s' % i] = bakje[i]
        result = list(index.itervalues())
        self.assertEquals(sorted(result), result)

    def tooBig_testDemonstrateSortedSegmentsOverlap(self):
        index = _Index("/data/seqstore_gc_perftest/big.index")
        # for i in xrange(32 * 10 ** 6):
        #     if i % 10000  == 0:
        #         print i
        #         from sys import stdout; stdout.flush()
        #     index['id%s' % i] = time() * 10 ** 6
        # index.commit()
        print index._index.writer.numDocs()
        from sys import stdout; stdout.flush()
        lastValue = 0
        for i, value in enumerate(index.itervalues()):
            if i % 10000 == 0:
                print i
                from sys import stdout; stdout.flush()
            if value < lastValue:
                self.fail("value %s < %s" % (value, lastValue))
            lastValue = value

    def testCopyTo(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        sequentialStorage.add(identifier='def', data="2")

        copyTarget = CallTrace()
        sequentialStorage.copyTo(target=copyTarget)

        self.assertEquals(['add', 'add'], copyTarget.calledMethodNames())