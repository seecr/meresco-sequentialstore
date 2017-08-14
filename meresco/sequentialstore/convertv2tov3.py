## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2017 Seecr (Seek You Too B.V.) http://seecr.nl
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

from os import rename
from os.path import join
from sys import stdout

from meresco.sequentialstore._previous import ForConversionOnlyV2SequentialStorage
from meresco.sequentialstore.sequentialstorage import SequentialStorage
from shutil import rmtree


def convertV2ToV3(directory):
    prev = ForConversionOnlyV2SequentialStorage(directory)
    newSeqStoreDir = join(directory + '.tmp')
    new = SequentialStorage(newSeqStoreDir)
    total = prev._lastKey  # estimate
    for i, (identifier, data, delete) in enumerate(prev.events()):
        progress(count=i+1, total=total, interval=1000)
        if delete:
            new.delete(identifier)
        else:
            new.add(identifier, data)
    new.close()
    prev.close()
    progress(count=i+1, total=total, interval=1)
    rmtree(directory)
    rename(newSeqStoreDir, directory)

def progress(count, total, interval, out=stdout):
    if count % interval != 0:
        return
    out.write("\rprogress: %.1f%%" % (100.0 * count / total))
