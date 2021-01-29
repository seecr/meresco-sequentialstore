## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2018, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
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

import sys
from zlib import compressobj, decompressobj


class Export(object):
    """Transfer mechanism to migrate data from older SequentialStore versions to newer ones (newer Lucene or indexed in another way)."""

    version = '1'
    VERSION_LINE = 'Export format version: %s\n' % version

    def __init__(self, path):
        self._path = path
        self._openFile = None
        self._compress = None

    def close(self):
        if not self._openFile is None:
            self._openFile.close()
            self._openFile = None

    def export(self, seqStorage):
        self._openFile = open(self._path, 'wb')
        self._openFile.write(self.VERSION_LINE.encode())
        self._compress = compressobj()
        size = len(seqStorage)
        self._openFile.write(('%s\n' % size).encode())
        for i, (identifier, data) in enumerate(seqStorage.iteritems()):
            if i % 1000 == 0:
                print('exporting item %s (%s%%)' % (i, (i * 100 / size)))
                sys.stdout.flush()
            self._writeItem(identifier, data)
        self._openFile.write(self._compress.flush())
        self._compress = None
        self.close()

    def importInto(self, seqStorage):
        self._openFile = open(self._path, 'rb')
        versionLine = self._openFile.readline().decode()
        assert self.VERSION_LINE == versionLine, "The SequentialStore export file does not match the expected version %s (%s)." % (self.version, repr(versionLine[:len(self.VERSION_LINE)]))
        size = int(self._openFile.readline())
        for i, (identifier, data) in enumerate(self._iteritems()):
            if i % 1000 == 0:
                print('importing item %s (%s%%)' % (i, (i * 100 / size)))
                sys.stdout.flush()
            seqStorage.add(identifier, data)
        self.close()

    def _writeItem(self, identifier, data):
        if '\n' in identifier:
            raise RuntimeError("Unexpectedly encountered \n character as part of the identifier '%s'." % identifier)
        bIdentifier = identifier.encode()
        if BOUNDARY_SENTINEL in bIdentifier:
            raise RuntimeError("Internal boundary sentinel unexpectedly appears as part of the identifier '%s'." % identifier)
        bData = data.encode()
        if BOUNDARY_SENTINEL in bData:
            raise RuntimeError("Internal boundary sentinel unexpectedly appears as part of the record data for identifier '%s'." % identifier)
        self._openFile.write(self._compress.compress(bIdentifier + b'\n'))
        self._openFile.write(self._compress.compress(bData))
        self._openFile.write(self._compress.compress(BOUNDARY_SENTINEL))

    def _iteritems(self):
        data = b''
        for s in self._decompress():
            data += s
            while True:
                record, sep, rest = data.partition(BOUNDARY_SENTINEL)
                if not sep:
                    break
                yield map(lambda x:x.decode(), record.split(b'\n', 1))
                data = rest

    def _decompress(self):
        decompress = decompressobj()
        for l in self._openFile:
            yield decompress.decompress(l)
        yield decompress.flush()


BOUNDARY_SENTINEL = b'\n=>> [{]} SequentialStore export record boundary {[}] <<=\n'  # Note: clearly this exact string must NEVER appear inside actual record data...
