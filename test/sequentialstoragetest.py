## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014-2015, 2017-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
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

from os.path import join, isfile
from shutil import rmtree
from subprocess import Popen, PIPE

from seecr.test import SeecrTestCase
from seecr.test.utils import sleepWheel

from meresco.sequentialstore import SequentialStorage
from time import time


class SequentialStorageTest(SeecrTestCase):

    def tearDown(self):
        from time import sleep
        sleep(0.05)
        SeecrTestCase.tearDown(self)

    def testAddGetItem(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data=b"1")
        self.assertEqual(b"1", sequentialStorage['abc'])
        self.assertEqual(1, len(sequentialStorage))

    def testAllBytes(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        everything = bytes(range(256))
        sequentialStorage.add(identifier="everything", data=everything)
        self.assertEqual(everything, sequentialStorage['everything'])
        sequentialStorage.commit()
        sequentialStorage.close()

        sequentialStorageRevisited = SequentialStorage(self.tempdir)
        self.assertEqual(everything, sequentialStorageRevisited['everything'])

    def testKeyErrorForUnknownKey(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        self.assertRaises(KeyError, lambda: sequentialStorage['abc'])

    def testPersisted(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data=b"1")
        sequentialStorage.add(identifier='def', data=b"2")
        self.assertEqual(2, len(sequentialStorage))
        sequentialStorage.commit()
        self.assertEqual(b"1", sequentialStorage['abc'])

        sequentialStorage.close()
        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        self.assertEqual(b"1", sequentialStorageReloaded['abc'])
        self.assertEqual(b"2", sequentialStorageReloaded['def'])
        self.assertEqual(2, len(sequentialStorageReloaded))
        self.assertEqual(["abc", "def"], list(sequentialStorageReloaded.iterkeys()))

    def testLenTakesDeletesIntoAccount(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data=b"1")
        sequentialStorage.delete(identifier='abc')
        self.assertEqual(0, len(sequentialStorage))

    def testGetMultiple(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data=b"1")
        sequentialStorage.add(identifier='def', data=b"2")
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        self.assertEqual([('abc', b'1'), ('def', b'2')], list(sequentialStorageReloaded.getMultiple(identifiers=['abc', 'def'])))

    def testGetMultipleCoercesToStr(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='1', data=b"x")
        sequentialStorage.add(identifier='2', data=b"y")
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        self.assertEqual([('1', b'x'), ('2', b'y')], list(sequentialStorageReloaded.getMultiple(identifiers=[1, 2])))

    def testDataNotRequiredToComplyEncoding(self):
        s = bytes([x for x in range(0, 256)]) * 3
        s = b'bytes\x01verder\x00\x01bytes'
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='432', data=s)
        self.assertEqual(s, sequentialStorage['432'])
        sequentialStorage.commit()
        self.assertEqual(s, sequentialStorage['432'])
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        self.assertEqual(s, sequentialStorageReloaded['432'])

    def testGetMultipleDifferentOrder(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='def', data=b"1")
        sequentialStorage.add(identifier='abc', data=b"2")
        self.assertEqual([('abc', b'2'), ('def', b'1')], list(sequentialStorage.getMultiple(identifiers=['abc', 'def'])))

    def testMultipleIgnoreMissing(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data=b"1")
        self.assertRaises(KeyError, lambda: list(sequentialStorage.getMultiple(identifiers=['abc', 'def'], ignoreMissing=False)))
        self.assertEqual([('abc', b'1')], list(sequentialStorage.getMultiple(identifiers=['abc', 'def'], ignoreMissing=True)))

    def testKeyMonotonicallyIncreasingAfterReopening(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data=b"1")
        sequentialStorage.add(identifier='def', data=b"2")
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        sequentialStorageReloaded.add(identifier='ghi', data=b"3")

        self.assertEqual(b"1", sequentialStorageReloaded['abc'])
        self.assertEqual(b"2", sequentialStorageReloaded['def'])
        self.assertEqual(b"3", sequentialStorageReloaded['ghi'])

    def testDelete(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data=b"1")
        sequentialStorage.add(identifier='def', data=b"2")
        sequentialStorage.delete(identifier='abc')
        self.assertRaises(KeyError, lambda: sequentialStorage['abc'])
        self.assertEqual(b'2', sequentialStorage['def'])

    def testDeleteAllowedForUnknownIdentifier(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='def', data=b"2")
        sequentialStorage.delete(identifier='abc')
        self.assertRaises(KeyError, lambda: sequentialStorage['abc'])

    def testDeletePersisted(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data=b"1")
        sequentialStorage.add(identifier='def', data=b"2")
        sequentialStorage.delete(identifier='abc')
        sequentialStorage.close()

        sequentialStorage = SequentialStorage(self.tempdir)
        self.assertRaises(KeyError, lambda: sequentialStorage['abc'])
        self.assertEqual(b'2', sequentialStorage['def'])

    def testClose(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data=b"1")
        lockFile = join(self.tempdir, 'write.lock')
        self.assertTrue(isfile(lockFile))
        sequentialStorage.close()
        with Popen("lsof -n %s" % lockFile, stdout=PIPE, stderr=PIPE, shell=True) as p:
            stdout, stderr = p.communicate()
        self.assertEqual(b'', stdout.strip())
        self.assertRaises(AttributeError, lambda: sequentialStorage.add('def', data=b'2'))

    def testGet(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        self.assertEqual(None, sequentialStorage.get('abc'))
        self.assertEqual('x', sequentialStorage.get(identifier='abc', default='x'))
        self.assertEqual('x', sequentialStorage.get('abc', 'x'))

    def testVersionWritten(self):
        SequentialStorage(self.tempdir)
        with open(join(self.tempdir, "sequentialstorage.version")) as fp:
            version = fp.read()
        self.assertEqual('5', version)

    def testRefuseInitInNonEmptyDirWithNoVersionFile(self):
        with open(join(self.tempdir, 'x'), 'w') as fp:
            pass
        try:
            SequentialStorage(self.tempdir)
            self.fail()
        except AssertionError as e:
            self.assertEqual("The %s directory is already in use for something other than a SequentialStorage." % self.tempdir, str(e))

    def testRefuseInitWithDifferentVersionFile(self):
        with open(join(self.tempdir, 'sequentialstorage.version'), 'w') as fp:
            fp.write('different version')
        try:
            SequentialStorage(self.tempdir)
            self.fail()
        except AssertionError as e:
            self.assertEqual('The SequentialStorage at %s needs to be converted to the current version.' % self.tempdir, str(e))

    def testRefuseInitWithDirectoryPathThatExistsAsFile(self):
        filePath = join(self.tempdir, 'x')
        with open(filePath, 'w') as fp:
            pass
        try:
            SequentialStorage(filePath)
            self.fail()
        except OSError as e:
            self.assertEqual("[Errno 17] File exists: '%s'" % filePath, str(e))

    def testShouldNotAllowOpeningTwice(self):
        SequentialStorage(self.tempdir)
        try:
            SequentialStorage(self.tempdir)
            self.fail()
        except Exception as e:
            self.assertTrue(repr(e).startswith('JavaError(<Throwable: org.apache.lucene.store.LockObtainFailedException: Lock held by this virtual machine: %s' % self.tempdir), e)

    def testIter(self):
        s = SequentialStorage(self.tempdir)
        for i in range(1000, 0, -1):
            s.add('identifier%s' % i, b'data%i' % i)
        for i in range(0, 1001, 2):
            s.delete('identifier%s' % i)
        expected = ['identifier%s' % i for i in range(999, 0, -2)]
        self.assertEqual(expected, list(iter(s)))
        self.assertEqual(expected, list(s.iterkeys()))
        expected = [b'data%i' % i for i in range(999, 0, -2)]
        self.assertEqual(expected, list(s.itervalues()))
        expected = [('identifier%s' % i, b'data%i' % i) for i in range(999, 0, -2)]
        self.assertEqual(expected, list(s.iteritems()))

    def testSignalConcurrentModification(self):
        s = SequentialStorage(self.tempdir)
        for i in range(999999):
            s.add('identifier%s' % i, b'data%i' % i)
        try:
            for i in s.iterkeys():
                s.delete(i)
            self.fail('should have failed with ConcurrentModificationException')
        except AssertionError as e:
            raise
        except Exception as e:
            self.assertEqual('java.util.ConcurrentModificationException: org.apache.lucene.store.AlreadyClosedException: this IndexReader is closed', str(e.getJavaException()))

    def testGcWithoutWait(self):
        directory = join(self.tempdir, 'store')
        for x in range(3):
            try:
                s = SequentialStorage(directory)
                for i in range(99999):
                    s.add('identifier%s' % i, b'data%i' % i)
                s.commit()
                size = s.getSizeOnDisk()
                self.assertTrue(size > 1000, size)
                for i in range(0, 99999, 3):  # delete some
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
        for x in range(3):
            try:
                s = SequentialStorage(directory)
                for i in range(99999):
                    s.add('identifier%s' % i, b'data%i' % i)
                s.commit()
                size = s.getSizeOnDisk()
                self.assertTrue(size > 1000, size)
                for i in range(0, 99999, 3):  # delete some
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
