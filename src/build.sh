set -o errexit

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

classpath=${luceneJarDir}/lucene-core-4.3.0.jar:${luceneJarDir}/lucene-analyzers-common-4.3.0.jar:${luceneJarDir}/lucene-facet-4.3.0.jar:${luceneJarDir}/lucene-queries-4.3.0.jar:${luceneJarDir}/lucene-misc-4.3.0.jar

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

