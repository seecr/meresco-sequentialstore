from os import getenv, makedirs, listdir
from warnings import warn
from os.path import join, isdir, isfile

from _sequentialstoragebynum import _SequentialStorageByNum

imported = False
def lazyImport():
    global imported
    if imported:
        return
    imported = True
    importVM()
    from java.io import File
    from org.apache.lucene.document import Document, StringField, Field, LongField, FieldType
    from org.apache.lucene.search import IndexSearcher, TermQuery
    from org.apache.lucene.index import DirectoryReader, Term, IndexWriter, IndexWriterConfig
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.util import Version
    from org.apache.lucene.analysis.core import WhitespaceAnalyzer

    StampType = FieldType()
    StampType.setIndexed(False)
    StampType.setStored(True)
    StampType.setNumericType(FieldType.NumericType.LONG)

    globals().update(locals())


class SequentialStorage(object):
    version = '1'

    def __init__(self, directory):
        self._directory = directory
        self._versionFormatCheck()
        self._index = _Index(join(directory, "index"))
        self._seqStorageByNum = _SequentialStorageByNum(join(directory, 'seqstore'))
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
        self._doc = Document()
        self._keyField = StringField("key", "", Field.Store.NO)
        self._valueField = LongField("value", 0L, StampType)
        self._doc.add(self._keyField)
        self._doc.add(self._valueField)

    def __setitem__(self, key, value):
        self._maybeReopen()
        self._keyField.setStringValue(key)
        self._valueField.setLongValue(long(value))
        self._writer.updateDocument(Term("key", key), self._doc)
        self._latestModifications[key] = value

    def __getitem__(self, key):
        value = self._latestModifications.get(key)
        if value == DELETED_RECORD:
            raise KeyError("Record deleted")
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

    def __delitem__(self, key):
        self._writer.deleteDocuments(Term("key", key))
        self._latestModifications[key] = DELETED_RECORD

    def _maybeReopen(self):
        if len(self._latestModifications) > 10000:
            self._reader = DirectoryReader.openIfChanged(self._reader, self._writer, True)
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
    #confif.setUseCompoundFile(false) # faster, for Lucene 4.4 and later
    writer = IndexWriter(directory, config)
    reader = writer.getReader()
    searcher = IndexSearcher(reader)
    return writer, reader, searcher

DELETED_RECORD = object()
