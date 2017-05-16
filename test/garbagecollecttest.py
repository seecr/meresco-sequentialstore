## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014, 2016-2017 Seecr (Seek You Too B.V.) http://seecr.nl
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

from seecr.test import SeecrTestCase
from seecr.test.io import stderr_replaced

from os import stat, makedirs, rename
from os.path import join, isfile
from time import time

from meresco.sequentialstore import SequentialStorage

from testutils import randomString


class GarbageCollectTest(SeecrTestCase):
    def testOne(self):
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        s.add('id:1', 'data1') # key: 1
        s.add('id:2', 'data2') # key: 2
        s.add('id:3', 'data3') # key: 3 current
        s.add('id:1', 'data4') # key: 4 current
        s.delete('id:2')       # key: 5

        self.assertEquals('data4', s['id:1'])
        self.assertRaises(KeyError, lambda: s['id:2'])
        self.assertEquals('data3', s['id:3'])

        current_keys = s._index._index.current_keys()
        self.assertEquals(False, current_keys.get(0))
        self.assertEquals(False, current_keys.get(1))
        self.assertEquals(False, current_keys.get(2))
        self.assertEquals(True, current_keys.get(3))
        self.assertEquals(True, current_keys.get(4))
        self.assertEquals(False, current_keys.get(5))

        s.gc()
        self.assertEquals('data4', s['id:1'])
        self.assertRaises(KeyError, lambda: s['id:2'])
        self.assertEquals('data3', s['id:3'])
        self.assertEquals([(3, '+id:3\ndata3'), (4, '+id:1\ndata4')],
                list(s._store.range(1)))

    def testLargerSequentialStorage(self):
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        for i in xrange(100):
            if i % 10 == 0:
                 print i
                 from sys import stdout; stdout.flush()
            s.add('id:%s' % i, 'data%s' % i)
            v = list(s._index.itervalues())
            self.assertEquals(i + 1, len(v))
            self.assertEquals(sorted(v), v)
        self.assertEquals('data99', s['id:99'])
        t0 = time()
        s.gc()
        self.assertEquals('data99', s['id:99'])
        self.assertTiming(0.01, time() - t0, 0.06)

    def testVerboseWithLargerGC(self):
        # Too small GC's won't test verbosity
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        for i in xrange(2700):
            s.add('id:%s' % i, 'data%s' % i)
        for i in xrange(1, 2700, 2):
            s.add('id:%s' % i, 'rewrite%s' % i)
        for i in xrange(5, 2700, 5):
            s.delete('id:%s' % i)
        self.assertEquals(2161, len(s._index))
        self.assertRaises(KeyError, lambda: s['id:2695'])
        self.assertEquals('data2698', s['id:2698'])
        self.assertEquals('rewrite2699', s['id:2699'])

        t0 = time()
        with stderr_replaced() as err:
            s.gc(verbose=True)
            result = err.getvalue()
        t1 = time()
        #self.assertEquals('''\
        #Progress:
        #\rIdentifiers (#2.000 of #2.161), NumericKeys (current 3.849, last 4.589)\
        #\rIdentifiers (#2.161 of #2.161), NumericKeys (current 4.050, last 4.589)
        #Finished garbage-collecting SequentialStorage.\n\n''', result)

        self.assertEquals(2161, len(s._index))
        self.assertRaises(KeyError, lambda: s['id:2695'])
        self.assertEquals('data2698', s['id:2698'])
        self.assertEquals('rewrite2699', s['id:2699'])

        #self.assertTiming(0.10, t1 - t0, 0.50) # with file copy
        self.assertTiming(0.01, t1 - t0, 0.02) # with lucene; most is reopen()

    def testPerformance(self):
        data = randomString(200)
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        for a in range(2):
            for i in xrange(10000):
                if i % 10 == 0:
                    print i
                    from sys import stdout; stdout.flush()
                s.add('id:%s' % i, data)
        t0 = time()

        from hotshot import Profile
        prof = Profile("/tmp/seqstore_gc.profile", lineevents=1, linetimings=1)
        try:
            prof.runcall(s.gc)
        finally:
            prof.close()

        print time() - t0
        #self.assertTiming(0.2, time() - t0, 0.5) # with file copy
        self.assertTiming(2.0, time() - t0, 3.0) # with lucene; most is reopen()

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
        print 'creating took %s' % (time() - t0)
        raw_input(self.tempdir)

        filesize = stat(filename).st_size
        print 'filesize', filesize

        t0 = time()
        s.gc()
        print "gc took %s" % (time() - t0)

        newFileSize = stat(filename).st_size
        print 'new filesize', newFileSize
        self.assertTrue(newFileSize < filesize)

        s = SequentialStorage(directory)
        self.assertEquals('data999', s['id:999'])
