from os import getenv, makedirs, listdir, rename
from warnings import warn
from os.path import join, isdir, isfile
from itertools import islice

from _sequentialstoragebynum import _SequentialStorageByNum

imported = False
def lazyImport():
    global imported
    if imported:
        return
    imported = True
    importVM()
    from java.lang import Long
    from java.io import File
    from org.apache.lucene.document import Document, StringField, Field, LongField, FieldType, NumericDocValuesField
    from org.apache.lucene.search import IndexSearcher, TermQuery, BooleanQuery, BooleanClause, MatchAllDocsQuery, NumericRangeQuery
    from org.apache.lucene.index import DirectoryReader, Term, IndexWriter, IndexWriterConfig
    from org.apache.lucene.index.sorter import SortingMergePolicy, NumericDocValuesSorter
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.util import Version
    from org.apache.lucene.analysis.core import WhitespaceAnalyzer

    from meresco_sequentialstore import initVM as initMerescoSequentialStore
    initMerescoSequentialStore()

    from org.meresco.sequentialstore import SeqStoreSortingCollector

    StampType = FieldType()
    StampType.setIndexed(True)
    StampType.setStored(True)
    StampType.setNumericType(FieldType.NumericType.LONG)

    globals().update(locals())


class SequentialStorage(object):
    version = '1'

    def __init__(self, directory):
        self._directory = directory
        self._versionFormatCheck()
        self._index = _Index(join(directory, INDEX_DIR))
        self._seqStorageByNum = _SequentialStorageByNum(join(directory, SEQSTORE_DIR))
        self._lastKey = self._seqStorageByNum.lastKey or 0

    def add(self, identifier, data):
        self._lastKey += 1
        key = self._lastKey
        self._seqStorageByNum.add(key=key, data=data)
        self._index[str(identifier)] = key  # only after actually writing data

    def delete(self, identifier):
        del self._index[str(identifier)]

    def __getitem__(self, identifier):
        key = self._index[str(identifier)]
        return self._seqStorageByNum[key]

    def get(self, identifier, default=None):
        try:
            return self[identifier]
        except KeyError:
            return default

    def getMultiple(self, identifiers, ignoreMissing=False):
        keys2Identifiers = dict()
        for identifier in identifiers:
            identifier = str(identifier)
            try:
                key = self._index[identifier]
            except KeyError:
                if not ignoreMissing:
                    raise
            else:
                keys2Identifiers[key] = identifier
        result = self._seqStorageByNum.getMultiple(keys=sorted(keys2Identifiers.keys()), ignoreMissing=ignoreMissing)
        return ((keys2Identifiers.get(key), data) for key, data in result)

    @classmethod
    def gc(cls, directory):
        """Works only for closed SequentialStorage for now."""
        if not isdir(join(directory, INDEX_DIR)) or not isfile(join(directory, SEQSTORE_DIR)):
            raise ValueError('Directory %s does not belong to a %s.' % (directory, cls))
        s = cls(directory)
        tmpSeqStoreFile = join(directory, 'seqstore~')
        tmpSequentialStorageByNum = _SequentialStorageByNum(tmpSeqStoreFile)
        existingNumKeys = s._index.itervalues()
        s._seqStorageByNum.copyTo(tmpSequentialStorageByNum, existingNumKeys)
        s.close()
        tmpSequentialStorageByNum.close()
        rename(tmpSeqStoreFile, join(directory, 'seqstore'))

    def close(self):
        self._seqStorageByNum.close()
        self._seqStorageByNum = None
        self._index.close()
        self._index = None

    def _versionFormatCheck(self):
        versionFile = join(self._directory, "sequentialstorage.version")
        if isdir(self._directory):
            assert (listdir(self._directory) == []) or (isfile(versionFile) and open(versionFile).read() == self.version), "The SequentialStorage at %s needs to be converted to the current version." % self._directory
        else:
            assert not isfile(self._directory), 'Given directory name %s exists as file.' % self._directory
            makedirs(self._directory)
        with open(versionFile, 'w') as f:
            f.write(self.version)


class _Index(object):
    def __init__(self, path):
        lazyImport()
        self._writer, self._reader, self._searcher = _getLucene(path)
        self._latestModifications = {}
        self._keyField = StringField("key", "", Field.Store.NO)
        self._valueField = LongField("value", 0L, StampType)

    def __setitem__(self, key, value):
        assert value > 0  # 0 has special meaning in this context
        self._maybeReopen()
        doc = Document()
        self._keyField.setStringValue(key)
        doc.add(self._keyField)
        self._valueField.setLongValue(long(value))
        doc.add(self._valueField)
        doc.add(NumericDocValuesField("value", long(value)))
        self._writer.updateDocument(Term("key", key), doc)
        self._latestModifications[key] = value

    def __getitem__(self, key):
        value = self._latestModifications.get(key)
        if value == DELETED_RECORD:
            raise KeyError(key)
        elif value is not None:
            return value
        self._maybeReopen()
        topDocs = self._searcher.search(TermQuery(Term("key", key)), 1)
        if topDocs.totalHits == 0:
            raise KeyError(key)
        return self._searcher.doc(topDocs.scoreDocs[0].doc).getField("value").numericValue().longValue()

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def itervalues(self):
        self._reopen()
        lastSeenKey = -1
        while True:
            lastSeenKey += 1
            query = NumericRangeQuery.newLongRange("value", lastSeenKey, Long.MAX_VALUE, True, False)
            collector = SeqStoreSortingCollector(2000)
            self._searcher.search(query, collector)
            if collector.totalHits() == 0:
                break
            for value in collector.collectedValues():
                if value == 0:  # Note: on the assumption that SequentialStorage is 1 based
                    break
                lastSeenKey = value
                yield value


    def __delitem__(self, key):
        self._writer.deleteDocuments(Term("key", key))
        self._latestModifications[key] = DELETED_RECORD

    def _maybeReopen(self):
        if len(self._latestModifications) > 10000:
            self._reopen()

    def _reopen(self):
        newReader = DirectoryReader.openIfChanged(self._reader, self._writer, True)
        if newReader:
            self._reader.close()
            self._reader = newReader
            self._searcher = IndexSearcher(self._reader)
            self._latestModifications.clear()

    def close(self):
        self._writer.close()


def importVM():
    maxheap = getenv('PYLUCENE_MAXHEAP')
    if not maxheap:
        maxheap = '4g'
        warn("Using '4g' as maxheap for lucene.initVM(). To override use PYLUCENE_MAXHEAP environment variable.")
    from lucene import initVM, getVMEnv
    try:
        VM = initVM(maxheap=maxheap)#, vmargs='-agentlib:hprof=heap=sites')
    except ValueError:
        VM = getVMEnv()
    return VM


def _getLucene(path):
    isdir(path) or makedirs(path)
    directory = FSDirectory.open(File(path))
    config = IndexWriterConfig(Version.LUCENE_43, None)
    config.setRAMBufferSizeMB(256.0) # faster
    #config.setUseCompoundFile(false) # faster, for Lucene 4.4 and later
    mergePolicy = config.getMergePolicy()
    sortingMergePolicy = SortingMergePolicy(mergePolicy, NumericDocValuesSorter("value", True))
    config.setMergePolicy(sortingMergePolicy)
    writer = IndexWriter(directory, config)
    reader = writer.getReader()
    searcher = IndexSearcher(reader)
    return writer, reader, searcher

DELETED_RECORD = object()
INDEX_DIR = 'index'
SEQSTORE_DIR = 'seqstore'
