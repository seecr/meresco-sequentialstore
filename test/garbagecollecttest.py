from os import stat
from os.path import join

from meresco.sequentialstore import SequentialStorage
from meresco.sequentialstore.garbagecollect import GarbageCollect

from seecr.test import SeecrTestCase


class GarbageCollectTest(SeecrTestCase):
    def testOne(self):
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        filename = s._seqStorageByNum._f.name
        s.add('id:1', 'data1')
        s.add('id:2', 'data2')
        s.add('id:3', 'data3')
        s.add('id:1', 'data4')
        s.delete('id:2')

        self.assertEquals('data4', s['id:1'])
        self.assertRaises(KeyError, lambda: s['id:2'])
        self.assertEquals('data3', s['id:3'])
        s.close()
        filesize = stat(filename).st_size

        GarbageCollect(directory).collect()

        newFileSize = stat(filename).st_size
        self.assertTrue(newFileSize < filesize)

        s = SequentialStorage(directory)
        self.assertEquals([(3, 'data3'), (4, 'data4')], list(s._seqStorageByNum.range()))

        self.assertEquals('data4', s['id:1'])
        self.assertRaises(KeyError, lambda: s['id:2'])
        self.assertEquals('data3', s['id:3'])

    def testLargerSequentialStorage(self):
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        for i in xrange(1000):
            s.add('id:%s' % i, 'data%s' % i)
        self.assertEquals('data999', s['id:999'])
        s.close()
        GarbageCollect(directory).collect()
        s = SequentialStorage(directory)
        self.assertEquals('data999', s['id:999'])

