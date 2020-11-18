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
        with stdout_replaced():
            N = 1023
            s = SequentialStorage(join(self.tempdir, 'store'))
            for i in range(N):
                s.add("identifier%s" % i, ''.join(chr(j % 255) for j in range(i)))
            Export(join(self.tempdir, 'export')).export(s)
            self.assertTrue(isfile(join(self.tempdir, 'export')))

            s = SequentialStorage(join(self.tempdir, 'store2'))
            Export(join(self.tempdir, 'export')).importInto(s)
            s.close()

            s = SequentialStorage(join(self.tempdir, 'store2'))
            for i, identifier in enumerate(s.iterkeys()):
                self.assertEqual('identifier%s' % i, identifier)
            self.assertEqual(N-1, i)
            for i in range(N):
                self.assertEqual(''.join(chr(j % 255) for j in range(i)), s.get('identifier%s' % i))

    def testSequentialStoreExportAndImport(self):
        with stdout_replaced():
            N = 19
            s = SequentialStorage(join(self.tempdir, 'store'))
            for i in range(N):
                s.add("identifier%s" % i, ''.join(chr(j % 255) for j in range(i)))
            s.export(join(self.tempdir, 'export'))
            self.assertTrue(isfile(join(self.tempdir, 'export')))

            s = SequentialStorage(join(self.tempdir, 'store2'))
            s.importFrom(join(self.tempdir, 'export'))
            s.close()

            s = SequentialStorage(join(self.tempdir, 'store2'))
            for i, identifier in enumerate(s.iterkeys()):
                self.assertEqual('identifier%s' % i, identifier)
            self.assertEqual(N-1, i)
            for i in range(N):
                self.assertEqual(''.join(chr(j % 255) for j in range(i)), s.get('identifier%s' % i))

    def testImportFromExportMustMatchVersion(self):
        with open(join(self.tempdir, 'export'), 'wb') as f:
            f.write(b'Export format version: 0\n')
        try:
            ex = Export(join(self.tempdir, 'export'))
            try:
                ex.importInto(None)
            finally:
                ex.close()
        except AssertionError as e:
            self.assertEqual("The SequentialStore export file does not match the expected version 1 ('Export format version: 0\\n').", str(e))
