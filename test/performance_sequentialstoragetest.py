## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014, 2017-2018, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from os import makedirs
from os.path import isdir, join
from shutil import rmtree
from time import time
from random import randint

from seecr.test import SeecrTestCase

from meresco.sequentialstore import SequentialStorage


class PerformanceSequentialStorageTest(SeecrTestCase):
    def testGetitem(self):
        N = 500000
        c = SequentialStorage(self.tempdir)
        H = "This is a holding-like record, at least, it tries to look like it, but I am not sure if it is really something that comes close enough. Anyway, here you go: Holding: %s"
        self.assertEqual(168, len(H))

        for i in range(N):
            c.add(identifier="http://nederland.nl/%s" % i, data=(H % i).encode())

        def f():
            t0 = time()
            for i in range(N):
                j = randint(0, N-1)
                data = c["http://nederland.nl/%s" % j]
                #self.assertEquals(H % j, data)
                if i % 1000 == 0:
                    t1 = time()
                    print(i, i/(t1-t0))
            print((time() - t0) / N)
        #from seecr.utils.profileit import profile
        #profile(f)
        f()

    def testSpeedAddsAndGetitems(self):
        N = 500000
        directory = join(self.tempdir, 'perfstore')
        if isdir(directory):
            rmtree(directory)
        makedirs(directory)


        def f():
            c = SequentialStorage(directory)
            H = "This is a holding-like record, at least, it tries to look like it, but I am not sure if it is really something that comes close enough. Anyway, here you go: Holding: %s"
            self.assertEqual(168, len(H))
            T = 0
            for i in range(N):
                identifier="http://nederland.nl/%s" % i
                data=H % i
                t0 = time()
                c.add(identifier=identifier, data=data.encode())
                T += (time() - t0)
                if i % 1000 == 0:
                    print(i, i / T)
            print("write", T / N)
            t1 = time()
            c.commit()
            print('commit took', time() - t1)
            c.close()
        #from seecr.utils.profileit import profile
        #profile(f)
        f()

        def iterkeys():
            clearCaches()
            c = SequentialStorage(directory)
            t0 = time()
            for i, identifier in enumerate(c.iterkeys()):
                if i % 1000 == 0:
                    print(i, i/(time() - t0))
            print("iterkeys", (time() - t0) / i)
            c.close()
        iterkeys()

        def iteritems():
            clearCaches()
            c = SequentialStorage(directory)
            t0 = time()
            for i, item in enumerate(c.iteritems()):
                if i % 1000 == 0:
                    print(i, i/(time() - t0))
            print("iteritems", (time() - t0) / i)
            c.close()
        iteritems()

        def sequentialRead():
            clearCaches()
            t0 = time()
            c = SequentialStorage(directory)
            print('opening for sequentialRead', (time() - t0))
            bytes = 0
            T = 0
            for i in range(N):
                identifier = "http://nederland.nl/%s" % i
                t0 = time()
                data = c[identifier]
                T += (time() - t0)
                bytes += len(data)
                if i % 1000 == 0:
                    print(i, i/T, bytes/T)
                    # print 'GC objects', gc.get_count()
            print("sequential read", T / N, T / bytes)
            c.close()
        sequentialRead()

        def randomRead():
            clearCaches()
            c = SequentialStorage(directory)
            bytes = 0
            T = 0
            for i in range(N):
                j = randint(0, N-1)
                identifier = "http://nederland.nl/%s" % j
                t0 = time()
                data = c[identifier]
                T += (time() - t0)
                bytes += len(data)
                if i % 1000 == 0:
                    print(i, i/T, bytes/T)
                    # print 'GC objects', gc.get_count()
            print("random read", T / N, T / bytes)
            c.close()
        randomRead()

        input('ready... ' + self.tempdir)


def clearCaches():
    input('''\nFile system caches need to be cleared before next performance test.\nAs root do:\n  # sync; echo 3 > /proc/sys/vm/drop_caches\n\n\nPress <Return> when ready...''')
