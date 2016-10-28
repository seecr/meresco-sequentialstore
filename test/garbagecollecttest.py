## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014, 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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
from meresco.sequentialstore.sequentialstorage import INDEX_DIR, SEQSTOREBYNUM_NAME
from meresco.sequentialstore.garbagecollect import garbageCollect

from testutils import randomString


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

        garbageCollect(directory)

        newFileSize = stat(filename).st_size
        self.assertTrue(newFileSize < filesize)

        s = SequentialStorage(directory)
        self.assertEquals([(3, '+id:3\ndata3'), (4, '+id:1\ndata4')], list(s._seqStorageByNum.range()))

        self.assertEquals('data4', s['id:1'])
        self.assertRaises(KeyError, lambda: s['id:2'])
        self.assertEquals('data3', s['id:3'])

    def testOnlyGcOnSequentialStorage(self):
        self.assertRaises(ValueError, lambda: garbageCollect(join(self.tempdir, 'x')))

        makedirs(join(self.tempdir, INDEX_DIR))
        self.assertRaises(ValueError, lambda: garbageCollect(join(self.tempdir)))

        open(join(self.tempdir, SEQSTOREBYNUM_NAME), 'w').close()
        self.assertRaises(AssertionError, lambda: garbageCollect(join(self.tempdir)))

        open(join(self.tempdir, 'sequentialstorage.version'), 'w').write('2')
        garbageCollect(join(self.tempdir))

    def testDontAppendOnPreviousInterruptedGC(self):
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        filename = s._seqStorageByNum._f.name
        s.add('id:1', 'data1')
        s.add('id:2', 'data2')
        s.close()
        filesizeBefore = stat(filename).st_size

        tmpSFilename = join(self.tempdir, 'store', 'seqstore~')
        open(tmpSFilename, 'w').write('I should be gone')
        self.assertTrue(isfile(tmpSFilename))

        garbageCollect(directory)

        self.assertFalse(isfile(tmpSFilename))
        newFileSize = stat(filename).st_size
        self.assertEquals(newFileSize, filesizeBefore)

        s = SequentialStorage(directory)
        self.assertEquals([(1, '+id:1\ndata1'), (2, '+id:2\ndata2')], list(s._seqStorageByNum.range()))

    def testLargerSequentialStorage(self):
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        for i in xrange(100):
            # if i % 10 == 0:
            #     print i
            #     from sys import stdout; stdout.flush()
            s.add('id:%s' % i, 'data%s' % i)
            v = list(s._index.itervalues())
            self.assertEquals(i + 1, len(v))
            self.assertEquals(sorted(v), v)
        self.assertEquals('data99', s['id:99'])
        s.close()
        t0 = time()
        garbageCollect(directory)
        s = SequentialStorage(directory)
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
        s.close()

        t0 = time()
        with stderr_replaced() as err:
            garbageCollect(directory=directory, verbose=True)
            result = err.getvalue()
        t1 = time()
        self.assertEquals('''\
Progress:
\rIdentifiers (#2000 of #2161), NumericKeys (current 3849, last 4589)\
\rIdentifiers (#2161 of #2161), NumericKeys (current 4050, last 4589)
Finished garbage-collecting SequentialStorage.\n\n''', result)

        s = SequentialStorage(directory)
        self.assertEquals(2161, len(s._index))
        self.assertRaises(KeyError, lambda: s['id:2695'])
        self.assertEquals('data2698', s['id:2698'])
        self.assertEquals('rewrite2699', s['id:2699'])

        self.assertTiming(0.10, t1 - t0, 0.50)

    def testTargetDirMustBeExistingDir(self):
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        s.close()
        try:
            garbageCollect(directory, targetDir=join(self.tempdir, 'does-not-exist'))
            self.fail()
        except ValueError, e:
            self.assertEquals("'targetDir' %s/does-not-exist is not an existing directory." % self.tempdir, str(e))

    def testSpecifiedTargetDir(self):
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

        targetDir = join(self.tempdir, 'target')
        makedirs(targetDir)
        with stderr_replaced() as err:
            garbageCollect(directory, targetDir=targetDir, verbose=True)
            self.assertEquals('''\
Progress:
\rIdentifiers (#2 of #2), NumericKeys (current 4, last 5)
To finish garbage-collecting the SequentialStorage, now replace '{0}/store/seqstore' with '{0}/target/seqstore' manually.\n\n'''.format(self.tempdir), err.getvalue())

        self.assertEquals(filesize, stat(filename).st_size)

        targetFilename = join(self.tempdir, 'target', 'seqstore')
        self.assertTrue(isfile(targetFilename))
        self.assertTrue(0 < stat(targetFilename).st_size < filesize)

        s = SequentialStorage(directory)
        self.assertEquals([(1, '+id:1\ndata1'), (2, '+id:2\ndata2'), (3, '+id:3\ndata3'), (4, '+id:1\ndata4'), (5, '-id:2\n')], list(s._seqStorageByNum.range()))

        self.assertEquals('data4', s['id:1'])
        self.assertRaises(KeyError, lambda: s['id:2'])
        self.assertEquals('data3', s['id:3'])
        s.close()

        rename(targetFilename, filename)  # the manual step to be taken by the administrator to finish the GC with 'targetDir'
        s = SequentialStorage(directory)
        self.assertEquals([(3, '+id:3\ndata3'), (4, '+id:1\ndata4')], list(s._seqStorageByNum.range()))

        self.assertEquals('data4', s['id:1'])
        self.assertRaises(KeyError, lambda: s['id:2'])
        self.assertEquals('data3', s['id:3'])
        s.close()

    def SKIP_testPerformance(self):
        data = randomString(200)
        directory = join(self.tempdir, 'store')
        s = SequentialStorage(directory)
        for a in range(2):
            for i in xrange(10000):
                # if i % 10 == 0:
                #     print i
                #     from sys import stdout; stdout.flush()
                s.add('id:%s' % i, data)
        s.close()
        t0 = time()

        gc = lambda: garbageCollect(directory)
        from hotshot import Profile
        prof = Profile("/tmp/seqstore_gc.profile", lineevents=1, linetimings=1)
        try:
            prof.runcall(gc)
        finally:
            prof.close()

        print time() - t0
        self.assertTiming(0.2, time() - t0, 0.5)

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
        raw_input(self.tempdir)

        filesize = stat(filename).st_size
        print 'filesize', filesize

        t0 = time()
        garbageCollect(directory)
        print "gc took %s" % (time() - t0)

        newFileSize = stat(filename).st_size
        print 'new filesize', newFileSize
        self.assertTrue(newFileSize < filesize)

        s = SequentialStorage(directory)
        self.assertEquals('data999', s['id:999'])
