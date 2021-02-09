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

from seecr.tools.build import buildIfNeeded                                     #DO_NOT_DISTRIBUTE
from os.path import join, dirname, abspath                                      #DO_NOT_DISTRIBUTE
try:                                                                            #DO_NOT_DISTRIBUTE
    buildIfNeeded(                                                              #DO_NOT_DISTRIBUTE
        soFilename=join(                                                        #DO_NOT_DISTRIBUTE
            "meresco_sequentialstore",                                          #DO_NOT_DISTRIBUTE
            "_meresco_sequentialstore.*.so"),                                   #DO_NOT_DISTRIBUTE
        buildCommand="cd {srcDir}; ./build.sh",                                 #DO_NOT_DISTRIBUTE
        findRootFor=abspath(__file__))                                          #DO_NOT_DISTRIBUTE
except RuntimeError as e:                                                       #DO_NOT_DISTRIBUTE
    print("Building failed!\n{}\n".format(str(e)))                              #DO_NOT_DISTRIBUTE
    exit(1)                                                                     #DO_NOT_DISTRIBUTE

from .__version__ import VERSION
from .adddeletetomultisequential import AddDeleteToMultiSequential
from .multisequentialstorage import MultiSequentialStorage
from .sequentialstorage import SequentialStorage
from .storagecomponentadapter import StorageComponentAdapter

from . import export
