## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015, 2017 Seecr (Seek You Too B.V.) http://seecr.nl
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

from meresco.sequentialstore import SequentialStorage
from meresco.sequentialstore.convertv2tov3 import convertV2ToV3
from meresco.sequentialstore._previous import ForConversionOnlyV2SequentialStorage

mydir = dirname(abspath(__file__))
testdatadir = join(mydir, 'data')
binDir = join(dirname(mydir), 'bin')


class ConvertV2ToV3Test(SeecrTestCase):
    def testOne(self):
        seqStoreDir = join(self.tempdir, 'seqStore')
        prev = ForConversionOnlyV2SequentialStorage(seqStoreDir)
        prev.add('id1', 'data1')
        prev.add('id2', 'data2')
        prev.add('id3', 'data3')
        prev.delete('id2')
        prev.close()

        # convertV2ToV3(seqStoreDir)
        logFile = join(self.tempdir, 'convertv2tov3.log')
        system('%s %s > %s 2>&1' % (join(binDir, 'sequentialstore_convert_v2_to_v3'), seqStoreDir, logFile))
        # print open(logFile).read()

        s = SequentialStorage(seqStoreDir)
        self.assertEquals('data1', s['id1'])
        self.assertRaises(KeyError, lambda: s['id2'])
        self.assertEquals('data3', s['id3'])
