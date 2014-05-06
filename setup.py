# -*- coding: utf-8 -*-
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

from distutils.core import setup
from os import walk
from os.path import isdir, join

packages = []
for path, dirs, files in walk('meresco'):
    if '__init__.py' in files and path != 'meresco':
        packages.append(path.replace('/', '.'))

data_files = []
if isdir('usr-share'):
    for path, dirs, files in walk('usr-share'):
            data_files.append((path.replace('usr-share', '/usr/share/meresco-sequentialstore', 1), [join(path, f) for f in files]))

scripts = []
if isdir('bin'):
    for path, dirs, files in walk('bin'):
        for file in files:
            scripts.append(join(path, file))

setup(
    name = 'meresco-sequentialstore',
    packages = [
        'meresco',                  #DO_NOT_DISTRIBUTE
    ] + packages,
    package_data={},
    scripts=scripts,
    data_files=data_files,
    version = '%VERSION%',
    url = 'http://seecr.nl',
    author = 'Seecr (Seek You Too B.V.)',
    author_email = 'info@seecr.nl',
    description = 'Meresco SequentialStore contains components facilitating efficient sequentially ordered storing and retrieval.',
    long_description = 'Meresco SequentialStore contains components facillitating efficient sequentially ordered storing and retrieval.',
    license = 'GPL',
    platforms='all',
)
