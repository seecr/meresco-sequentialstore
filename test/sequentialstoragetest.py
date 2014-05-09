from seecr.test import SeecrTestCase

from meresco.sequentialstore import SequentialStorage


class SequentialStorageTest(SeecrTestCase):
    def testOne(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        self.assertEquals("1", sequentialStorage['abc'])

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

    def testGetMultipleDifferentOrder(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='def', data="1")
        sequentialStorage.add(identifier='abc', data="2")
        self.assertEquals([('def', '1'), ('abc', '2')], list(sequentialStorage.getMultiple(identifiers=['abc', 'def'])))

    def testStampMonotonicallyIncreasingAfterReopening(self):
        sequentialStorage = SequentialStorage(self.tempdir)
        sequentialStorage.add(identifier='abc', data="1")
        sequentialStorage.add(identifier='def', data="2")
        sequentialStorage.close()

        sequentialStorageReloaded = SequentialStorage(self.tempdir)
        sequentialStorageReloaded.add(identifier='ghi', data="3")

        self.assertEquals("1", sequentialStorageReloaded['abc'])
        self.assertEquals("2", sequentialStorageReloaded['def'])
        self.assertEquals("3", sequentialStorageReloaded['ghi'])

