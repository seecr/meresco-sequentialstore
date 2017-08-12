## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014-2015, 2017 Seecr (Seek You Too B.V.) http://seecr.nl
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
from random import shuffle
from subprocess import Popen, PIPE
from shutil import rmtree

from seecr.test import SeecrTestCase, CallTrace

from meresco.sequentialstore import SequentialStorage


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
        sequentialStorage.commit()
        self.assertEquals("1", sequentialStorage['abc'])

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

    def testDataNotRequiredToComplyEncoding(self):
        s = ''.join(chr(x) for x in range(0, 256)) * 3
        # s = ''.join(chr(0) for x in range(0, 256)) * 3
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
        self.assertEquals('3', version)

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
        except OSError, e:
            self.assertEquals("[Errno 17] File exists: '%s'" % filePath, str(e))

    def testShouldNotAllowOpeningTwice(self):
        SequentialStorage(self.tempdir)
        try:
            SequentialStorage(self.tempdir)
            self.fail()
        except Exception, e:
            self.assertTrue(repr(e).startswith('JavaError(<Throwable: org.apache.lucene.store.LockObtainFailedException: Lock held by this virtual machine: %s' % self.tempdir), e)
