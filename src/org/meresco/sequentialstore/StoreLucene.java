/* begin license *
 *
 * "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
 *
 * Copyright (C) 2017 Seecr (Seek You Too B.V.) http://seecr.nl
 *
 * This file is part of "Meresco SequentialStore"
 *
 * "Meresco SequentialStore" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco SequentialStore" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco SequentialStore"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

package org.meresco.sequentialstore;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;


public class StoreLucene {
    private DirectoryReader reader;
    private IndexWriter writer;
    private IndexSearcher searcher;
    private long newestKey = 0;

    private StringField _identifierField;
    private BinaryDocValuesField _identifierDocValueField;
    private StoredField _storedKeyField;
    private NumericDocValuesField _numericKeyField;
    private BinaryDocValuesField _dataField;
    private Document _doc;

    private static String _IDENTIFIER_FIELD = "identifier";
    private static String _IDENTIFIER_DOC_VALUE_FIELD = "identifier";
    private static String _KEY_FIELD = "key";
    private static String _NUMERIC_KEY_FIELD = "key";
    private static String _DATA_FIELD = "data";


    public StoreLucene(String path) throws IOException {
        Directory directory = FSDirectory.open(Paths.get(path));
        IndexWriterConfig config = new IndexWriterConfig();
        config.setRAMBufferSizeMB(256.0); // faster
        config.setUseCompoundFile(false); // faster, for Lucene 4.4 and later

        config.setIndexSort(new Sort(new SortField(_NUMERIC_KEY_FIELD, SortField.Type.LONG)));
        this.writer = new IndexWriter(directory, config);
        this.reader = DirectoryReader.open(this.writer, false, false);
        this.searcher = new IndexSearcher(this.reader);

        this.newestKey = newestKeyFromIndex();

        this._identifierField = new StringField(_IDENTIFIER_FIELD, "", Field.Store.NO);
        this._identifierDocValueField = new BinaryDocValuesField(_IDENTIFIER_DOC_VALUE_FIELD, new BytesRef());
        this._storedKeyField = new StoredField(_KEY_FIELD, 0L);
        this._numericKeyField = new NumericDocValuesField(_NUMERIC_KEY_FIELD, 0L);
        this._dataField = new BinaryDocValuesField(_DATA_FIELD, new BytesRef());
        this._doc = new Document();
        this._doc.add(this._identifierField);
        this._doc.add(this._identifierDocValueField);
        this._doc.add(this._storedKeyField);
        this._doc.add(this._numericKeyField);
        this._doc.add(this._dataField);
    }

    public void reopen() throws IOException {
        DirectoryReader newReader = DirectoryReader.openIfChanged(this.reader, this.writer, true);
        if (newReader != null) {
            this.reader.close();
            this.reader = newReader;
            this.searcher = new IndexSearcher(this.reader);
        }
    }

    public int numDocs() {
        return this.writer.numDocs();
    }

    public void commit() throws IOException {
        this.writer.commit();
    }

    public void close() {
        if (this.writer != null) {
            try {
                this.writer.close();
            } catch (IOException e) {
            } finally {
                this.writer = null;
            }
        }
        if (this.reader != null) {
            try {
                this.reader.close();
            } catch (IOException e) {
            } finally {
                this.reader = null;
                this.searcher = null;
            }
        }
    }

    public void add(String identifier, BytesRef data) throws IOException {
        long newKey = newKey();
        this._identifierField.setStringValue(identifier);
        this._identifierDocValueField.setBytesValue(new BytesRef(identifier));
        this._storedKeyField.setLongValue(newKey);
        this._numericKeyField.setLongValue(newKey);
        this._dataField.setBytesValue(data);
        this.writer.updateDocument(new Term(_IDENTIFIER_FIELD, identifier), this._doc);
    }

    public void delete(String identifier) throws IOException {
        this.writer.deleteDocuments(new Term(_IDENTIFIER_FIELD, identifier));
    }

    public String getData(String identifier) throws IOException {
        TopDocs results = searcher.search(new TermQuery(new Term(_IDENTIFIER_FIELD, identifier)), 1);
        if (results.totalHits == 0) {
            return null;
        }
        int docId = results.scoreDocs[0].doc;
        List<LeafReaderContext> leaves = this.reader.leaves();
        LeafReaderContext readerContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
        return _getData(docId, readerContext);
    }

    private long newKey() {
        this.newestKey += 1;
        return this.newestKey;
    }

    private long newestKeyFromIndex() throws IOException {
        int maxDoc = this.searcher.getIndexReader().maxDoc();
        if (maxDoc < 1) {
            return 0;
        }
        Document doc = this.searcher.doc(maxDoc - 1);
        if (doc == null) {
            return 0;
        }
        long newestKey = doc.getField(_KEY_FIELD).numericValue().longValue();
        return newestKey;
    }

    public PyIterator<String> iterkeys() throws IOException {
        // Requires reopen to be called first.
        PyIterator<Item> items = iteritems(true, false);
        return new PyIterator<String>() {
            @Override
            public String next() {
                Item item = items.next();
                return item != null ? item.identifier : null;
            }
        };
    }

    public PyIterator<String> itervalues() throws IOException {
        // Requires reopen to be called first.
        PyIterator<Item> items = iteritems(false, true);
        return new PyIterator<String>() {
            @Override
            public String next() {
                Item item = items.next();
                return item != null ? item.data : null;
            }
        };
    }

    public PyIterator<Item> iteritems() throws IOException {
        // Requires reopen to be called first.
        return iteritems(true, true);
    }

    private PyIterator<Item> iteritems(boolean includeIdentifier, boolean includeData) throws IOException {
        return new PyIterator<Item>() {
            IndexReader currentReader = null;
            List<LeafReaderContext> leaves;
            Bits liveDocs;
            int maxDoc;
            int docId = 0;

            @Override
            public Item next() {
                if (!StoreLucene.this.reader.equals(currentReader)) {
                    this.currentReader = StoreLucene.this.reader;
                    leaves = StoreLucene.this.reader.leaves();
                    liveDocs = MultiFields.getLiveDocs(currentReader);
                    maxDoc = currentReader.maxDoc();
                }
                String identifier = null;
                String data = null;
                while (identifier == null && data == null) {
                    if (docId >= maxDoc) {
                        return null;
                    }
                    if (liveDocs == null || liveDocs.get(docId)) {
                        LeafReaderContext readerContext = leaves.get(ReaderUtil.subIndex(this.docId, leaves));
                        try {
                            if (includeIdentifier) {
                                BinaryDocValues identifierBinaryDocValues = readerContext.reader().getBinaryDocValues(_IDENTIFIER_DOC_VALUE_FIELD);
                                identifier = identifierBinaryDocValues.get(docId - readerContext.docBase).utf8ToString();
                            }
                            if (includeData) {
                                data = _getData(docId, readerContext);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    docId++;
                }
                return new Item(identifier, data);
            }
        };
    }

    private String _getData(int docId, LeafReaderContext readerContext) throws IOException {
        BinaryDocValues dataBinaryDocValues = readerContext.reader().getBinaryDocValues(_DATA_FIELD);
        BytesRef bytesRef = dataBinaryDocValues.get(docId - readerContext.docBase);
        byte[] bytes = bytesRef.bytes;
        if (bytesRef.offset > 0 || bytesRef.length != bytes.length) {
            bytes = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);
        }
        return Base64.getEncoder().encodeToString(bytes);
    }

    public interface PyIterator<T> {
        public T next();
    }

    public class Item {
        public String identifier;
        public String data;

        Item(String identifier, String data) {
            this.identifier = identifier;
            this.data = data;
        }
    }
}