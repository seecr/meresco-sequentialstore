from os import stat
from os.path import join
from time import time

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
            if i % 100 == 0:
                print i
                from sys import stdout; stdout.flush()
            s.add('id:%s' % i, 'data%s' % i)
            v = list(s._index.itervalues())
            self.assertEquals(i + 1, len(v))
            self.assertEquals(sorted(v), v)
        self.assertEquals('data999', s['id:999'])
        s.close()
        GarbageCollect(directory).collect()
        s = SequentialStorage(directory)
        self.assertEquals('data999', s['id:999'])

    def TAKES_ABOUT_15_MINUTES_testRatherLargeSequentialStorage(self):
        print 'Needs 210MB'
        from sys import stdout; stdout.flush()

        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        filename = s._seqStorageByNum._f.name
        t0 = time()
        for a in range(2):
            for i in xrange(2000000):
                if i % 10000 == 0:
                    print i
                    from sys import stdout; stdout.flush()
                s.add('id:%s' % i, 'data%s' % i)
        s.close()
        print 'creating took %s' % (time() - t0)
        filesize = stat(filename).st_size
        print 'filesize', filesize

        t0 = time()
        GarbageCollect(directory).collect()
        print "gc took %s" % (time() - t0)

        newFileSize = stat(filename).st_size
        print 'new filesize', newFileSize
        self.assertTrue(newFileSize < filesize)

        s = SequentialStorage(directory)
        self.assertEquals('data999', s['id:999'])

    def testOnlyGcOnSequentialStorage(self):
        self.assertRaises(ValueError, lambda: GarbageCollect(join(self.tempdir, 'x')).collect())
