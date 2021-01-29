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

from os.path import dirname, abspath, join, isfile                               #DO_NOT_DISTRIBUTE
from os import stat, system                                                      #DO_NOT_DISTRIBUTE
from glob import glob                                                            #DO_NOT_DISTRIBUTE
from sys import exit, path as sysPath                                            #DO_NOT_DISTRIBUTE
mydir = dirname(abspath(__file__))                                               #DO_NOT_DISTRIBUTE
srcDir = join(dirname(dirname(mydir)), 'src')                                    #DO_NOT_DISTRIBUTE
libDir = join(dirname(dirname(mydir)), 'lib')                                    #DO_NOT_DISTRIBUTE
sofilename = "_meresco_sequentialstore.cpython-37m-x86_64-linux-gnu.so"          #DO_NOT_DISTRIBUTE
sofile = join(libDir, 'meresco_sequentialstore', sofilename)                     #DO_NOT_DISTRIBUTE
javaSources = glob(join(srcDir, 'org','meresco','sequentialstore', '*.java'))    #DO_NOT_DISTRIBUTE
if javaSources:                                                                  #DO_NOT_DISTRIBUTE
    lastMtime = max(stat(f).st_mtime for f in javaSources)                       #DO_NOT_DISTRIBUTE
    if not isfile(sofile) or stat(sofile).st_mtime < lastMtime:                  #DO_NOT_DISTRIBUTE
        result = system('cd %s; ./build.sh %s' % (srcDir, libDir))               #DO_NOT_DISTRIBUTE
        if result:                                                               #DO_NOT_DISTRIBUTE
            exit(result)                                                         #DO_NOT_DISTRIBUTE
sysPath.insert(0, libDir)                                                        #DO_NOT_DISTRIBUTE

from .__version__ import VERSION
from .adddeletetomultisequential import AddDeleteToMultiSequential
from .multisequentialstorage import MultiSequentialStorage
from .sequentialstorage import SequentialStorage
from .storagecomponentadapter import StorageComponentAdapter

from . import export
