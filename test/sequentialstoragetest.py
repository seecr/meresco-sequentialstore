## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014-2015, 2017-2019 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
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

from os.path import join, isfile
from shutil import rmtree
from subprocess import Popen, PIPE

from seecr.test import SeecrTestCase
from seecr.test.utils import sleepWheel

from meresco.sequentialstore import SequentialStorage
from sys import stdout
from time import time


class SequentialStorageTest(SeecrTestCase):
    def testAddGetItem(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        self.assertEquals("1", sequentialStorage['abc'])
        self.assertEquals(1, len(sequentialStorage))

    def testKeyErrorForUnknownKey(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        self.assertRaises(KeyError, lambda: sequentialStorage['abc'])

    def testPersisted(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        sequentialStorage.add(identifier='def', data="2")
        self.assertEquals(2, len(sequentialStorage))
        sequentialStorage.commit()
        self.assertEquals("1", sequentialStorage['abc'])

        sequentialStorage.close()
        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        self.assertEquals("1", sequentialStorageReloaded['abc'])
        self.assertEquals("2", sequentialStorageReloaded['def'])
        self.assertEquals(2, len(sequentialStorageReloaded))
        self.assertEquals(["abc", "def"], list(sequentialStorageReloaded.iterkeys()))

    def testLenTakesDeletesIntoAccount(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        sequentialStorage.delete(identifier='abc')
        self.assertEquals(0, len(sequentialStorage))

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

    def testDataNotRequiredToComplyEncoding(self):
        s = ''.join(chr(x) for x in range(0, 256)) * 3
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='432', data=s)
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        self.assertEquals(s, sequentialStorageReloaded['432'])

    def testGetMultipleDifferentOrder(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='def', data="1")
        sequentialStorage.add(identifier='abc', data="2")
        self.assertEquals([('abc', '2'), ('def', '1')], list(sequentialStorage.getMultiple(identifiers=['abc', 'def'])))

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

    def testDeleteAllowedForUnknownIdentifier(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='def', data="2")
        sequentialStorage.delete(identifier='abc')
        self.assertRaises(KeyError, lambda: sequentialStorage['abc'])

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
        lockFile = join(self.tempdir, 'write.lock')
        self.assertTrue(isfile(lockFile))
        sequentialStorage.close()
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
        self.assertEquals('4', version)

    def testRefuseInitInNonEmptyDirWithNoVersionFile(self):
        open(join(self.tempdir, 'x'), 'w').close()
        try:
            SequentialStorage(self.tempdir)
            self.fail()
        except AssertionError, e:
            self.assertEquals("The %s directory is already in use for something other than a SequentialStorage." % self.tempdir, str(e))

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
        except OSError, e:
            self.assertEquals("[Errno 17] File exists: '%s'" % filePath, str(e))

    def testShouldNotAllowOpeningTwice(self):
        SequentialStorage(self.tempdir)
        try:
            SequentialStorage(self.tempdir)
            self.fail()
        except Exception, e:
            self.assertTrue(repr(e).startswith('JavaError(<Throwable: org.apache.lucene.store.LockObtainFailedException: Lock held by this virtual machine: %s' % self.tempdir), e)

    def testIter(self):
        s = SequentialStorage(self.tempdir)
        for i in xrange(1000, 0, -1):
            s.add('identifier%s' % i, 'data%s' % i)
        for i in xrange(0, 1001, 2):
            s.delete('identifier%s' % i)
        expected = ['identifier%s' % i for i in xrange(999, 0, -2)]
        self.assertEquals(expected, list(iter(s)))
        self.assertEquals(expected, list(s.iterkeys()))
        expected = ['data%s' % i for i in xrange(999, 0, -2)]
        self.assertEquals(expected, list(s.itervalues()))
        expected = [('identifier%s' % i, 'data%s' % i) for i in xrange(999, 0, -2)]
        self.assertEquals(expected, list(s.iteritems()))

    def testSignalConcurrentModification(self):
        s = SequentialStorage(self.tempdir)
        for i in xrange(99999):
            s.add('identifier%s' % i, 'data%s' % i)
        try:
            for i in s.iterkeys():
                s.delete('identifier%s' % i)
            self.fail('should have failed with ConcurrentModificationException')
        except AssertionError, e:
            raise
        except Exception, e:
            self.assertEquals('java.util.ConcurrentModificationException: org.apache.lucene.store.AlreadyClosedException: this IndexReader is closed', str(e.getJavaException()))

    def testGcWithoutWait(self):
        directory = join(self.tempdir, 'store')
        for x in xrange(3):
            try:
                s = SequentialStorage(directory)
                for i in xrange(99999):
                    s.add('identifier%s' % i, 'data%s' % i)
                s.commit()
                size = s.getSizeOnDisk()
                self.assertTrue(size > 1000, size)
                for i in xrange(0, 99999, 3):  # delete some
                    s.delete('identifier%s' % i)
                s.commit()
                newSize = s.getSizeOnDisk()
                self.assertTrue(newSize >= size, (newSize, size))

                s.gc()
                newSizeAfterGcStart = s.getSizeOnDisk()
                self.assertTrue(newSizeAfterGcStart >= newSize, (newSizeAfterGcStart, newSize))  # grows a little initially
                s.commit()
                newSize = s.getSizeOnDisk()
                self.assertTrue(newSize >= newSizeAfterGcStart, (newSize, newSizeAfterGcStart))

                sleepWheel(1)
                s.commit()
                newSize = s.getSizeOnDisk()
                self.assertTrue(newSize < newSizeAfterGcStart, (newSize, newSizeAfterGcStart))
            finally:
                s.close()
                rmtree(directory)

    def testGcWithWait(self):
        directory = join(self.tempdir, 'store')
        for x in xrange(3):
            try:
                s = SequentialStorage(directory)
                for i in xrange(99999):
                    s.add('identifier%s' % i, 'data%s' % i)
                s.commit()
                size = s.getSizeOnDisk()
                self.assertTrue(size > 1000, size)
                for i in xrange(0, 99999, 3):  # delete some
                    s.delete('identifier%s' % i)
                s.commit()
                newSize = s.getSizeOnDisk()
                self.assertTrue(newSize >= size, (newSize, size))
                t = time()
                s.gc(doWait=True)
                took = time() - t
                self.assertTrue(took < 0.5, took)
                sizeAfterGc = s.getSizeOnDisk()
                self.assertTrue(sizeAfterGc < newSize, (sizeAfterGc, newSize))
            finally:
                s.close()
                rmtree(directory)
