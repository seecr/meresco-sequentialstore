## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

from os import system
from os.path import dirname, abspath, join
from shutil import copytree

from meresco.sequentialstore import SequentialStorage


mydir = dirname(abspath(__file__))
testdatadir = join(mydir, 'data')
binDir = join(dirname(mydir), 'bin')


class ConvertV1ToV2Test(SeecrTestCase):
    def testOne(self):
        seqStoreDir = join(self.tempdir, 'seqStore')
        copytree(join(testdatadir, 'v1SeqStore'), seqStoreDir)
        logFile = join(self.tempdir, 'convertv1tov2.log')
        system('%s %s > %s 2>&1' % (join(binDir, 'sequentialstore_convert_v1_to_v2'), seqStoreDir, logFile))

        # print open(logFile).read()

        s = SequentialStorage(seqStoreDir)
        self.assertEquals('data1', s['id1'])
        self.assertRaises(KeyError, lambda: s['id2'])
        self.assertEquals('data3', s['id3'])
