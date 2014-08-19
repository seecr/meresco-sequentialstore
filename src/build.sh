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

set -o errexit

if ! javac -version 2>&1 | grep 1.7 > /dev/null; then
    echo "javac should be java 7"
    exit 1
fi

mydir=$(cd $(dirname $0); pwd)
buildDir=$mydir/build
libDir=$1
if [ -z "$libDir" ]; then
    libDir=$(dirname $mydir)/lib
fi

rm -rf $buildDir
mkdir $buildDir
rm -rf $libDir
mkdir -p $libDir

luceneJarDir=/usr/lib64/python2.6/site-packages/lucene
if [ -f /etc/debian_version ]; then
    luceneJarDir=/usr/lib/python2.7/dist-packages/lucene
fi

LUCENE_VERSION=4.9.0
classpath=${luceneJarDir}/lucene-core-${LUCENE_VERSION}.jar:${luceneJarDir}/lucene-analyzers-common-${LUCENE_VERSION}.jar:${luceneJarDir}/lucene-facet-${LUCENE_VERSION}.jar:${luceneJarDir}/lucene-queries-${LUCENE_VERSION}.jar:${luceneJarDir}/lucene-misc-${LUCENE_VERSION}.jar

javac -cp ${classpath} -d ${buildDir} org/meresco/sequentialstore/*.java
(cd $buildDir; jar -c org > $buildDir/meresco-sequentialstore.jar)

python -m jcc.__main__ \
    --root $mydir/root \
    --use_full_names \
    --import lucene \
    --shared \
    --arch x86_64 \
    --jar $buildDir/meresco-sequentialstore.jar \
    --python meresco_sequentialstore \
    --build \
    --install

rootLibDir=$mydir/root/usr/lib64/python2.6/site-packages/meresco_sequentialstore
if [ -f /etc/debian_version ]; then
    rootLibDir=$mydir/root/usr/local/lib/python2.7/dist-packages/meresco_sequentialstore
fi

mv ${rootLibDir} $libDir/


rm -rf $buildDir
rm -rf $mydir/root
rm -rf $mydir/meresco_sequentialstore.egg-info

