## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2014-2017 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
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

from os import getenv, makedirs, listdir, rename, remove
from os.path import join, isdir, isfile
from shutil import rmtree
from warnings import warn
from itertools import islice


imported = False
def importVM():
    maxheap = getenv('PYLUCENE_MAXHEAP')
    if not maxheap:
        maxheap = '4g'
        warn("Using '4g' as maxheap for lucene.initVM(). To override use PYLUCENE_MAXHEAP environment variable.")
    from lucene import initVM, getVMEnv
    try:
        VM = initVM(maxheap=maxheap) #, vmargs='-agentlib:hprof=heap=sites')
    except ValueError:
        VM = getVMEnv()
    return VM


def lazyImport():
    global imported
    if imported:
        return
    importVM()

    from lucene import JArray
    from java.io import File
    from java.nio.file import Paths
    from org.apache.lucene.document import Document, StringField, Field, LongPoint, StoredField, NumericDocValuesField, BinaryDocValuesField
    from org.apache.lucene.search import IndexSearcher, TermQuery, Sort, SortField
    from org.apache.lucene.index import DirectoryReader, Term, IndexWriter, IndexWriterConfig, ReaderUtil
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.util import Version, BytesRef

    imported = True
    globals().update(locals())


class SequentialStorage(object):
    version = '3'

    def __init__(self, directory, maxModifications=None):
        lazyImport()
        self._directory = directory
        if not isdir(directory):
            makedirs(directory)
        self._versionFormatCheck()
        self._maxModifications = DEFAULT_MAX_MODIFICATIONS if maxModifications is None else maxModifications
        self._writer, self._reader, self._searcher = self._getLucene()
        self._latestModifications = {}
        self.gets = 0
        self.cacheHits = 0
        self._newestKey = self._newestKeyFromIndex()

        self._identifierField = StringField(_IDENTIFIER_FIELD, "", Field.Store.NO)
        self._storedKeyField = StoredField(_KEY_FIELD, 0L)
        self._numericKeyField = NumericDocValuesField(_NUMERIC_KEY_FIELD, 0L)
        self._dataField = BinaryDocValuesField(_DATA_FIELD, BytesRef())
        self._doc = Document()
        self._doc.add(self._identifierField)
        self._doc.add(self._storedKeyField)
        self._doc.add(self._numericKeyField)
        self._doc.add(self._dataField)

    def add(self, identifier, data):
        if identifier is None:
            raise ValueError('identifier should not be None')
        if data is None:
            raise ValueError('data should not be None')
        identifier = str(identifier)
        data = str(data)
        newKey = self._newKey()
        self._identifierField.setStringValue(identifier)
        self._storedKeyField.setLongValue(newKey)
        self._numericKeyField.setLongValue(newKey)
        self._dataField.setBytesValue(pyStrToBytesRef(data))
        self._writer.updateDocument(Term(_IDENTIFIER_FIELD, identifier), self._doc)
        self._latestModifications[identifier] = data
        if len(self._latestModifications) > self._maxModifications:
            self.commit()

    __setitem__ = add

    def delete(self, identifier):
        identifier = str(identifier)
        self._writer.deleteDocuments(Term(_IDENTIFIER_FIELD, identifier))
        self._latestModifications[identifier] = DELETED_RECORD

    __delitem__ = delete

    def __getitem__(self, identifier):
        self.gets += 1
        identifier = str(identifier)
        value = self._latestModifications.get(identifier)
        if not value is None:
            self.cacheHits += 1
            if value is DELETED_RECORD:
                raise KeyError(identifier)
            return value
        data = self._getData(identifier)
        if data is None:
            raise KeyError(identifier)
        return str(data)

    def get(self, identifier, default=None):
        try:
            return self[identifier]
        except KeyError:
            return default

    def getMultiple(self, identifiers, ignoreMissing=False):
        for identifier in identifiers:
            identifier = str(identifier)
            try:
                data = self[identifier]
            except KeyError:
                if ignoreMissing:
                    continue
                raise
            yield identifier, data

    # desirable: iteritems, iterkeys, itervalues

    def close(self):
        if self._writer is None:
            return
        self.commit()
        self._writer.close()
        self._writer = None
        self._reader.close()
        self._searcher = None
        self._reader = None

    def commit(self):
        self._writer.commit()
        self._reopen()


    def _newKey(self):
        self._newestKey += 1L
        return self._newestKey

    def _newestKeyFromIndex(self):
        searcher = self._getSearcher()
        maxDoc = searcher.getIndexReader().maxDoc()
        if maxDoc < 1:
            return 0L
        doc = searcher.doc(maxDoc - 1)
        if doc is None:
            return 0L
        newestKey = doc.getField(_KEY_FIELD).numericValue().longValue()
        return newestKey

    def _getData(self, identifier):
        docId = self._getDocId(identifier)
        leaves = self._reader.leaves()
        readerContext = leaves.get(ReaderUtil.subIndex(docId, leaves))
        dataBinaryDocValues = readerContext.reader().getBinaryDocValues(_DATA_FIELD);
        if dataBinaryDocValues is None:
            raise KeyError(identifier);
        dataBytesRef = dataBinaryDocValues.get(docId - readerContext.docBase)
        return bytesRefToPyStr(dataBytesRef)

    def _getDocId(self, identifier):
        searcher = self._getSearcher(identifier)
        results = searcher.search(TermQuery(Term(_IDENTIFIER_FIELD, identifier)), 1)
        if results.totalHits == 0:
            raise KeyError(identifier)
        return results.scoreDocs[0].doc

    def _getSearcher(self, identifier=None):
        modifications = len(self._latestModifications)
        if modifications == 0:
            return self._searcher
        if identifier and str(identifier) not in self._latestModifications and modifications < self._maxModifications:
            return self._searcher
        self._reopen()
        return self._searcher

    def _reopen(self):
        newreader = DirectoryReader.openIfChanged(self._reader, self._writer, True)
        if newreader:
            self._reader.close()
            self._reader = newreader
            self._searcher = IndexSearcher(newreader)
            # print 'clear latest modifications'
            # import sys; sys.stdout.flush()
            self._latestModifications.clear()

    def _versionFormatCheck(self):
        versionFile = join(self._directory, "sequentialstorage.version")
        if isdir(self._directory):
            assert (listdir(self._directory) == []) or (isfile(versionFile) and open(versionFile).read() == self.version), "The SequentialStorage at %s needs to be converted to the current version." % self._directory
        else:
            assert not isfile(self._directory), 'Given directory name %s exists as file.' % self._directory
            makedirs(self._directory)
        with open(versionFile, 'w') as f:
            f.write(self.version)

    def _getLucene(self):
        directory = FSDirectory.open(Paths.get(self._directory))
        config = IndexWriterConfig()
        config.setRAMBufferSizeMB(256.0) # faster
        config.setUseCompoundFile(False) # faster, for Lucene 4.4 and later
        # TODO: set max segment size?

        config.setIndexSort(Sort(SortField(_NUMERIC_KEY_FIELD, SortField.Type.LONG)));
        writer = IndexWriter(directory, config)
        reader = writer.getReader()
        searcher = IndexSearcher(reader)
        return writer, reader, searcher


def pyStrToBytesRef(s):
    return BytesRef(JArray('byte')(s))

def bytesRefToPyStr(bytesRef):
    return ''.join(chr(b & 0xFF) for b in islice(bytesRef.bytes, bytesRef.offset, bytesRef.length))


DEFAULT_MAX_MODIFICATIONS = 10000

_IDENTIFIER_FIELD = "identifier"
_KEY_FIELD = "key"
_NUMERIC_KEY_FIELD = "key"
_DATA_FIELD = "data"

DELETED_RECORD = object()
