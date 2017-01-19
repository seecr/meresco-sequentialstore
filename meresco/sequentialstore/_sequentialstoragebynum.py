## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014-2015, 2017 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Stichting Kennisnet http://www.kennisnet.nl
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

from os.path import getsize, abspath
from zlib import compress, decompress, error as ZlibError
from math import ceil
import operator
import sys


class _SequentialStorageByNum(object):
    def __init__(self, fileName, file_=None, blockSize=None):
        # Note that file_ argument should only be used in a test situation.
        self._f = file_ or open(fileName, "ab+")
        blockSize = blockSize or DEFAULT_BLOCK_SIZE
        self._blkIndex = _BlkIndex(self, blockSize)
        self.lastKey = None
        targetBlk = lastBlk = self._blkIndex.search(LARGER_THAN_ANY_KEY)
        while True:
            try:
                self.lastKey = self._blkIndex.scan(targetBlk, last=True)
            except StopIteration:
                pass
            if self.lastKey is None:
                if targetBlk > 0:
                    targetBlk -= 1
                    continue
                assert lastBlk == 0, '%s not recognized as SequentialStorage (or all data corrupt).' % abspath(fileName)
            break

    def add(self, key, data, alreadyCompressed=False):
        _intcheck(key)
        if key <= self.lastKey:
            raise ValueError("key %s must be greater than last key %s" % (key, self.lastKey))
        self.lastKey = key
        if not alreadyCompressed:
            data = compress(data)
        record = RECORD % dict(key=key, length=len(data), data=data, sentinel=SENTINEL)
        self._f.write(record)
        self._blkIndex.adjustSize(len(record))

    def __getitem__(self, key):
        _intcheck(key)
        blk = self._blkIndex.search(key)
        try:
            found_key, data = self._blkIndex.scan(blk, target_key=key)
        except StopIteration:
            raise IndexError
        return data

    def range(self, start=0, stop=None, inclusive=False):
        stop = stop or LARGER_THAN_ANY_KEY
        _intcheck(start); _intcheck(stop)
        cmp = operator.le if inclusive else operator.lt
        blk = self._blkIndex.search(start)
        key, data = self._blkIndex.scan(blk, target_key=start, greater=True)
        offset = self._f.tell()
        while cmp(key, stop):
            yield key, data
            self._f.seek(offset)
            key, data = self._readNext()
            offset = self._f.tell()

    def getMultiple(self, keys, ignoreMissing=False):
        offset = None
        prev_blk = None
        prev_key = None
        for key in keys:
            _intcheck(key)
            if not prev_key < key:
                raise ValueError('Keys should have been sorted.')

            blk = self._blkIndex.search(key, lo=prev_blk or 0)
            try:
                if self._blkIndex.offset(blk) > offset:
                    key, data = self._blkIndex.scan(blk, target_key=key)
                else:
                    if offset:
                        self._f.seek(offset)
                    key, data = self._readNext(target_key=key)
                offset = self._f.tell()
            except StopIteration:
                if ignoreMissing:
                    continue
                raise KeyError(key)

            yield key, data

            prev_blk = blk
            prev_key = key

    def copyTo(self, target, keys, skipDataCheck=False, verbose=False):
        def progressMsg(key):
            msg = '\rIdentifiers (#%s of #%s), NumericKeys (current %s, last %s)' % (thousandsformat(count), thousandsformat(length) if length is not None else 'unknown', thousandsformat(key), thousandsformat(self.lastKey))
            sys.stderr.write(msg)
            sys.stderr.flush()

        length = getattr(keys, 'length', None)
        keys = iter(keys)
        nextKey = next(keys, None)
        if nextKey is None:
            return
        blk = self._blkIndex.search(nextKey)
        self._f.seek(self._blkIndex.offset(blk))
        count = 0
        if verbose: sys.stderr.write('Progress:\n')
        while not nextKey is None:
            try:
                originalKey, data = self._readNext(target_key=nextKey, _keepCompressed=True, _keepCompressedVerifyData=not skipDataCheck)
            except StopIteration:
                raise RuntimeError('key %s not found.' % nextKey)
            if verbose:
                count += 1
                if count % 2000 == 0:
                    progressMsg(key=nextKey)
            target.add(key=nextKey, data=data, alreadyCompressed=True)
            nextKey = next(keys, None)
        if verbose:
            progressMsg(key=originalKey)
            sys.stderr.write('\n')
            sys.stderr.flush()

    def __iter__(self):
        self._f.seek(0)
        while True:
            yield self._readNext()

    def close(self):
        self._f.close()

    def flush(self):
        self._f.flush()

    def _readNext(self, target_key=None, keyOnly=False, greater=False, last=False, _keepCompressed=False, _keepCompressedVerifyData=True):
        # Use _keepCompressed and _keepCompressedVerifyData only internally, breaks recovery logic and abstractions.
        line = "sentinel not yet found"
        key = None; data = None; lastKey = None
        while line != '':
            line = self._f.readline()
            retryPosition = self._f.tell()
            if line.endswith(SENTINEL + '\n'):
                data = None
                try:
                    key = int(self._f.readline().strip())
                    length = int(self._f.readline().strip())
                except ValueError:
                    self._f.seek(retryPosition)
                    continue
                if keyOnly:
                    return key
                if target_key:
                    if key < target_key:
                        continue
                    elif not greater and key != target_key:
                        raise StopIteration
                rawdata = self._f.read(length)
                try:
                    if _keepCompressed:
                        if _keepCompressedVerifyData:
                            decompress(rawdata)
                        data = rawdata
                    else:
                        data = decompress(rawdata)
                except ZlibError:
                    self._f.seek(retryPosition)
                    continue
                retryPosition = self._f.tell()
                expectingNewline = self._f.read(1)  # newline after data
                if expectingNewline != '\n':
                    self._f.seek(retryPosition)
                if last:
                    lastKey = key
                    continue
                return key, data
        if last and not lastKey is None:
            return lastKey
        raise StopIteration


DEFAULT_BLOCK_SIZE = 8192
SENTINEL = "----"
RECORD = "%(sentinel)s\n%(key)s\n%(length)s\n%(data)s\n"
LARGER_THAN_ANY_KEY = 2**64

def thousandsformat(n):
    try:
        return format(n, ',').replace(',', '.')
    except ValueError:
        return n

class _BlkIndex(object):
    def __init__(self, src, blk_size):
        self._src = src
        self._blk_size = blk_size
        self._cache = {}
        self._size = getsize(src._f.name)

    def __getitem__(self, blk):
        key = self._cache.get(blk)
        if not key:
            try:
                key = self._cache[blk] = self.scan(blk, keyOnly=True)
            except StopIteration:
                raise IndexError
        return key

    def adjustSize(self, s):
        self._size += s

    def __len__(self):
        return ceil(self._size / float(self._blk_size))

    def scan(self, blk, **kwargs):
        self._src._f.seek(blk * self._blk_size)
        return self._src._readNext(**kwargs)

    def search(self, key, lo=0):
        return max(_bisect_left(self, key, lo=lo)-1, 0)

    def offset(self, blk):
        return blk * self._blk_size


def _intcheck(value):
    if not isinstance(value, (int, long)):
        raise ValueError('Expected int')


# from Python lib
def _bisect_left(a, x, lo=0, hi=None):
    """Return the index where to insert item x in list a, assuming a is sorted.

    The return value i is such that all e in a[:i] have e < x, and all e in
    a[i:] have e >= x.  So if x already appears in the list, a.insert(x) will
    insert just before the leftmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """

    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo+hi)//2
        try: # EG
            if a[mid] < x:
                lo = mid+1
            else: hi = mid
        except IndexError: #EG
            hi = mid #EG
    return lo
