## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
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

from os import makedirs, system
from os.path import join, dirname, abspath, isdir

from weightless.core import consume
from meresco.oai import OaiJazz

from meresco.sequentialstore import MultiSequentialStorage
from meresco.sequentialstore._sequentialstoragebynum import _SequentialStorageByNum
from seecr.test.io import stdout_replaced


mypath = dirname(abspath(__file__))
binDir = join(dirname(mypath), 'bin')
if not isdir(binDir):
    binDir = '/usr/bin'

class ConvertSeqstoreOAItov1Test(SeecrTestCase):
    @stdout_replaced
    def testConvert(self):
        # contruct starting point
        stateDir = join(self.tempdir, 'state')
        oaiJazz = OaiJazz(join(stateDir, 'oai'))

        makedirs(join(stateDir, 'sequential-store'))
        s = _SequentialStorageByNum(join(stateDir, 'sequential-store', 'rdf'))

        oaiJazz.addOaiRecord('abc', sets=[('aSet', 'a set')], metadataFormats=[('rdf', '', '')])
        stamp = oaiJazz.getRecord('abc').stamp
        s.add(stamp, "DATA" * 10)

        oaiJazz.addOaiRecord("def", sets=[('aSet', 'a set')], metadataFormats=[('rdf', '', '')])
        stamp = oaiJazz.getRecord('def').stamp
        s.add(stamp, "DATA" * 10)

        oaiJazz.addOaiRecord('abc', sets=[('aSet', 'a set')], metadataFormats=[('rdf', '', '')])
        stamp = oaiJazz.getRecord('abc').stamp
        s.add(stamp, "OTHER" * 10)

        oaiJazz.addOaiRecord('ghi', sets=[('aSet', 'a set')], metadataFormats=[('rdf', '', '')])
        stamp = oaiJazz.getRecord('ghi').stamp
        s.add(stamp, "DATA" * 10)

        consume(oaiJazz.delete("ghi"))

        for i in xrange(210):
            oaiJazz.addOaiRecord('id%s' % i, sets=[], metadataFormats=[('rdf', '', '')])
            stamp = oaiJazz.getRecord('id%s' % i).stamp
            s.add(stamp, "ABC" * 100)

        s.close()
        oaiJazz.close()
        # now in starting point state

        logFile = join(self.tempdir, 'convert_seqstore_OAI_to_v1.log')
        system("%s %s > %s 2>&1" % (
                join(binDir, 'convert_seqstore_OAI_to_v1'),
                stateDir,
                join(logFile),
            ))
        output = open(logFile).read()
        self.assertFalse('Traceback' in output, output)
        print output
        from sys import stdout; stdout.flush()

        mss = MultiSequentialStorage(join(stateDir, 'store'))
        self.assertEquals('OTHER' * 10, mss.getData(identifier='abc', name='rdf'))
        self.assertEquals('DATA' * 10, mss.getData(identifier='def', name='rdf'))
        self.assertRaises(KeyError, lambda: mss.getData(identifier='ghi', name='rdf'))

        self.assertEquals("ABC" * 100, mss.getData(identifier='id209', name='rdf'))
