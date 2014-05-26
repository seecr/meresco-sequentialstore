from seecr.test import SeecrTestCase

from meresco.sequentialstore import SequentialStorage

from time import time, sleep
from random import randint


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

