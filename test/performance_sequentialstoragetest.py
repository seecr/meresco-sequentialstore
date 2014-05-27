from os.path import join
from time import time
from random import randint

from seecr.test import SeecrTestCase
from testutils import randomString

from meresco.sequentialstore import SequentialStorage


class PerformanceSequentialStorageTest(SeecrTestCase):
    def testGetitem(self):
        N = 50000
        c = SequentialStorage(self.tempdir)
        H = "This is a holding-like record, at least, it tries to look like it, but I am not sure if it is really something that comes close enough. Anyway, here you go: Holding: %s"
        self.assertEquals(168, len(H))

        for i in xrange(N):
            c.add(identifier="http://nederland.nl/%s" % i, data=H % i)

        def f():
            t0 = time()
            for i in xrange(N):
                j = randint(0, N-1)
                data = c["http://nederland.nl/%s" % j]
                #self.assertEquals(H % j, data)
                if i % 1000 == 0:
                    t1 = time()
                    print i, i/(t1-t0)
            print (time() - t0) / N
        #from seecr.utils.profileit import profile
        #profile(f)
        f()

    def testSpeedAddsAndGetitems(self):
        N = 50000
        c = SequentialStorage(self.tempdir)
        H = "This is a holding-like record, at least, it tries to look like it, but I am not sure if it is really something that comes close enough. Anyway, here you go: Holding: %s"
        self.assertEquals(168, len(H))
        def f():
            t0 = time()
            for i in xrange(N):
                c.add(identifier="http://nederland.nl/%s" % i, data=H % i)
                j = randint(0, i)
                data = c["http://nederland.nl/%s" % j]
                #self.assertEquals(H % j, data)
                if i % 1000 == 0:
                    t1 = time()
                    print i, i/(t1-t0)
        #from seecr.utils.profileit import profile
        #profile(f)
        f()

    def testIterValues(self):
        N = 50000
        c = SequentialStorage(self.tempdir)
        H = "This is a holding-like record, at least, it tries to look like it, but I am not sure if it is really something that comes close enough. Anyway, here you go: Holding: %s"
        self.assertEquals(168, len(H))
        for i in xrange(N):
            c.add(identifier="http://nederland.nl/%s" % i, data=H % i)
        print 'built store with index'
        from sys import stdout; stdout.flush()

        M = 100
        def f():
            t0 = time()
            for i in xrange(M):
                l = list(c._index.itervalues())
            print 'list itervalues took on avg.', (time() - t0) / M
            self.assertEquals(N, len(l))
            self.assertEquals(l, sorted(l))
        #from seecr.utils.profileit import profile
        #profile(f)
        f()

    def testMicroPerformance(self):
        # ...
        sequentialStorage = SequentialStorage(self.tempdir)
        mockData = randomString(500)
        t0 = time()
        for i in xrange(50000):
            sequentialStorage.add(identifier=str(i), data=mockData)
        print '>>> ss.add()', time() - t0

        t0 = time()
        list(sequentialStorage._index.itervalues())
        print '>>> list(ss._index.itervalues()) - warm', time() - t0
        sequentialStorage.close()

        sequentialStorage = SequentialStorage(self.tempdir)
        t0 = time()
        list(sequentialStorage._index.itervalues())
        print '>>> list(ss._index.itervalues()) - cold', time() - t0

        from meresco.sequentialstore._sequentialstoragebynum import _SequentialStorageByNum
        s = _SequentialStorageByNum(self.tempfile)
        t0 = time()
        for i in xrange(50000):
            s.add(key=i, data=mockData)
        print '>>> ss_bynum.add()', time() - t0

        s2 = _SequentialStorageByNum(join(self.tempdir, 's2'))
        t0 = time()
        sequentialStorage._seqStorageByNum.copyTo(target=s2, keys=sequentialStorage._index.itervalues(), skipDataCheck=False)
        print '>>> ss_bynum.copyTo()', time() - t0

        s3 = _SequentialStorageByNum(join(self.tempdir, 's3'))
        t0 = time()
        sequentialStorage._seqStorageByNum.copyTo(target=s3, keys=sequentialStorage._index.itervalues(), skipDataCheck=True)
        print '>>> ss_bynum.copyTo(skipDataCheck=True)', time() - t0

        sequentialStorage.close()
        s.close()
        s2.close()
        s3.close()

