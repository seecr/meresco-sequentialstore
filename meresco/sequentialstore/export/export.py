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

from zlib import compressobj, decompressobj


class Export(object):
    """Transfer mechanism to migrate data from older SequentialStore versions to newer ones (newer Lucene or indexed in another way)."""

    version = '1'
    VERSION_LINE = 'Export format version: %s\n' % version

    def __init__(self, path, mode='r'):
        self._path = path
        assert mode in 'rw', "mode should be either 'r' or 'w'"
        self._mode = mode
        self._openFile = None
        self._compress = None

    def write(self, identifier, data):
        if self._mode != 'w':
            raise RuntimeError("writing to an export that was not opened in 'w' mode")
        if '\n' in identifier:
            raise RuntimeError("Unexpectedly encountered \n character as part of the identifier '%s'." % identifier)
        if BOUNDARY_SENTINEL in identifier:
            raise RuntimeError("Internal boundary sentinel unexpectedly appears as part of the identifier '%s'." % identifier)
        if BOUNDARY_SENTINEL in data:
            raise RuntimeError("Internal boundary sentinel unexpectedly appears as part of the record data for identifier '%s'." % identifier)
        if self._openFile is None:
            self._open()
        self._openFile.write(self._compress.compress(identifier + '\n'))
        self._openFile.write(self._compress.compress(data))
        self._openFile.write(self._compress.compress(BOUNDARY_SENTINEL))

    def __iter__(self):
        data = ''
        for s in self._decompress():
            data += s
            while True:
                record, sep, rest = data.partition(BOUNDARY_SENTINEL)
                if not sep:
                    break
                yield record.split('\n', 1)
                data = rest

    def close(self):
        if self._openFile is None:
            return
        if self._mode == 'w' and not self._compress is None:
            self._openFile.write(self._compress.flush())
            self._compress = None
        self._openFile.close()
        self._openFile = None

    def _open(self):
        self._openFile = open(self._path, self._mode)
        if self._mode == 'w':
            self._openFile.write(self.VERSION_LINE)
            self._compress = compressobj()
        else:
            versionLine = self._openFile.readline()
            assert self.VERSION_LINE == versionLine, "The SequentialStore export file does not match the expected version %s (%s)." % (self.version, repr(versionLine[:len(self.VERSION_LINE)]))

    def _decompress(self):
        if self._mode != 'r':
            raise RuntimeError("reading from an export that was not opened in 'r' mode")
        if self._openFile is None:
            self._open()
        decompress = decompressobj()
        for l in self._openFile:
            yield decompress.decompress(l)
        yield decompress.flush()

    def __enter__(self):
        self._open()
        return self

    def __exit__(self, *args):
        self.close()


BOUNDARY_SENTINEL = '\n=>> [{]} SequentialStore export record boundary {[}] <<=\n'  # Note: clearly this exact string must NEVER appear inside actual record data...
