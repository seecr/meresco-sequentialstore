from seecr.test import SeecrTestCase

from os import makedirs, rename, system
from os.path import join, dirname, abspath, isdir
from shutil import rmtree

from weightless.core import consume
from meresco.oai import OaiJazz

from meresco.sequentialstore import SequentialStorage, MultiSequentialStorage
from meresco.sequentialstore.sequentialstorage import SEQSTOREBYNUM_NAME


mypath = dirname(abspath(__file__))
binDir = join(dirname(mypath), 'bin')
if not isdir(binDir):
    binDir = '/usr/bin'

class ConvertSeqstoreOAItov1Test(SeecrTestCase):
    def testConvert(self):
        # contruct original layout with new object
        stateDir = join(self.tempdir, 'state')
        oaiJazz = OaiJazz(join(stateDir, 'oai'))

        s = SequentialStorage(join(stateDir, 'fixture'))
        s.add("abc", "DATA" * 10)
        oaiJazz.addOaiRecord('abc', sets=[('aSet', 'a set')], metadataFormats=[('rdf', '', '')])

        s.add("def", "DATA" * 10)
        oaiJazz.addOaiRecord('def', sets=[('aSet', 'a set')], metadataFormats=[('rdf', '', '')])

        s.add("abc", "OTHER" * 10)
        oaiJazz.addOaiRecord('abc', sets=[('aSet', 'a set')], metadataFormats=[('rdf', '', '')])

        s.add("ghi", "DATA" * 10)
        oaiJazz.addOaiRecord('ghi', sets=[('aSet', 'a set')], metadataFormats=[('rdf', '', '')])

        s.delete("ghi")
        consume(oaiJazz.delete("ghi"))
        s.close()
        oaiJazz.close()

        makedirs(join(stateDir, 'sequential-store'))
        rename(join(stateDir, 'fixture', SEQSTOREBYNUM_NAME), join(stateDir, 'sequential-store', 'rdf'))
        rmtree(join(stateDir, 'fixture'))

        # now in original state
        #raw_input(stateDir)

        logFile = join(self.tempdir, 'convert_seqstore_OAI_to_v1.log')
        system("%s %s > %s 2>&1" % (
                join(binDir, 'convert_seqstore_OAI_to_v1'),
                stateDir,
                join(logFile),
            ))

        print open(logFile).read()
        from sys import stdout; stdout.flush()

        mss = MultiSequentialStorage(join(stateDir, 'store'))
        self.assertEquals('OTHER' * 10, mss.getData(identifier='abc', name='rdf'))
        self.assertEquals('DATA' * 10, mss.getData(identifier='def', name='rdf'))
        self.assertRaises(KeyError, mss.getData(identifier='ghi', name='rdf'))
