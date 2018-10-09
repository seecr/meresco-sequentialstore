## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2018 Seecr (Seek You Too B.V.) http://seecr.nl
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
from seecr.test.io import stdout_replaced

from os.path import join, isfile

from meresco.sequentialstore import SequentialStorage
from meresco.sequentialstore.export import Export


class ExportTest(SeecrTestCase):
    def testExport(self):
        N = 1023
        s = SequentialStorage(join(self.tempdir, 'store'))
        for i in xrange(N):
            s.add("identifier%s" % i, ''.join(chr(j % 255) for j in xrange(i)))
        # print 'done filling seqstore, now building export'
        with Export(join(self.tempdir, 'export'), 'w') as exp:
            for identifier, data in s.iteritems():
                exp.write(identifier, data)
        self.assertTrue(isfile(join(self.tempdir, 'export')))

        # print 'building new seqstore from export'
        s = SequentialStorage(join(self.tempdir, 'store2'))
        with Export(join(self.tempdir, 'export')) as exp:
            for identifier, data in exp:
                s.add(identifier, data)
        s.close()

        # print 'verifying new seqstore'
        s = SequentialStorage(join(self.tempdir, 'store2'))
        for i, identifier in enumerate(s.iterkeys()):
            self.assertEquals('identifier%s' % i, identifier)
        self.assertEquals(N-1, i)
        # print 'verified identifiers'
        for i in xrange(N):
            # if i % 1000 == 0:
                # print i
                # import sys; sys.stdout.flush()
            self.assertEquals(''.join(chr(j % 255) for j in xrange(i)), s.get('identifier%s' % i))

    def testExportForReadingMustExist(self):
        try:
            exp = Export(join(self.tempdir, 'export'))
            for identifier, data in exp:
                print identifier
        except IOError, e:
            self.assertEquals("[Errno 2] No such file or directory: '%s/export'" % self.tempdir, str(e))

    def testExportForReadingMustMatchVersion(self):
        with open(join(self.tempdir, 'export'), 'w') as f:
            f.write('Export format version: 0\n')
        try:
            exp = Export(join(self.tempdir, 'export'))
            for identifier, data in exp:
                print identifier
        except AssertionError, e:
            self.assertEquals("The SequentialStore export file does not match the expected version 1 ('Export format version: 0\\n').", str(e))

    def testExportForReadingForbidsWriting(self):
        with open(join(self.tempdir, 'export'), 'w') as f:
            f.write('x')
        exp = Export(join(self.tempdir, 'export'))
        try:
            exp.write('id0', 'will not work')
        except RuntimeError, e:
            self.assertEquals("writing to an export that was not opened in 'w' mode", str(e))

    def testExportForWritingForbidsReading(self):
        exp = Export(join(self.tempdir, 'export'), 'w')
        try:
            for identifier, data in exp:
                print identifier
        except RuntimeError, e:
            self.assertEquals("reading from an export that was not opened in 'r' mode", str(e))

    def testSequentialStoreExportAndImport(self):
        with stdout_replaced():
            N = 19
            s = SequentialStorage(join(self.tempdir, 'store'))
            for i in xrange(N):
                s.add("identifier%s" % i, ''.join(chr(j % 255) for j in xrange(i)))
            s.export(join(self.tempdir, 'export'))
            self.assertTrue(isfile(join(self.tempdir, 'export')))

            s = SequentialStorage(join(self.tempdir, 'store2'))
            s.import_(join(self.tempdir, 'export'))
            s.close()

            s = SequentialStorage(join(self.tempdir, 'store2'))
            for i, identifier in enumerate(s.iterkeys()):
                self.assertEquals('identifier%s' % i, identifier)
            self.assertEquals(N-1, i)
            for i in xrange(N):
                self.assertEquals(''.join(chr(j % 255) for j in xrange(i)), s.get('identifier%s' % i))
