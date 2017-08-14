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
import java.util.List;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;


public class StoreLucene {
    private DirectoryReader reader;
    private IndexWriter writer;
    private IndexSearcher searcher;
    private long newestKey = 0;

    private StringField _identifierField;
    private StoredField _storedKeyField;
    private NumericDocValuesField _numericKeyField;
    private BinaryDocValuesField _dataField;
    private Document _doc;

    private static String _IDENTIFIER_FIELD = "identifier";
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
        this._storedKeyField = new StoredField(_KEY_FIELD, 0L);
        this._numericKeyField = new NumericDocValuesField(_NUMERIC_KEY_FIELD, 0L);
        this._dataField = new BinaryDocValuesField(_DATA_FIELD, new BytesRef());
        this._doc = new Document();
        this._doc.add(this._identifierField);
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
        this._storedKeyField.setLongValue(newKey);
        this._numericKeyField.setLongValue(newKey);
        this._dataField.setBytesValue(data);
        this.writer.updateDocument(new Term(_IDENTIFIER_FIELD, identifier), this._doc);
    }

    public void delete(String identifier) throws IOException {
        this.writer.deleteDocuments(new Term(_IDENTIFIER_FIELD, identifier));
    }

    public BytesRef getData(String identifier) throws IOException {
        TopDocs results = searcher.search(new TermQuery(new Term(_IDENTIFIER_FIELD, identifier)), 1);
        if (results.totalHits == 0) {
            return null;
        }
        int docId = results.scoreDocs[0].doc;
        List<LeafReaderContext> leaves = this.reader.leaves();
        LeafReaderContext readerContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
        BinaryDocValues dataBinaryDocValues = readerContext.reader().getBinaryDocValues(_DATA_FIELD);
        if (dataBinaryDocValues == null) {
            return null;
        }
        return dataBinaryDocValues.get(docId - readerContext.docBase);
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


    //    public PyIterator<String> iterkeys() throws IOException {
    //        // Needs this.reopen()
    //        List<AtomicReaderContext> leaves = this.reader.leaves();
    //        ReaderSlice[] readerSlices = new ReaderSlice[leaves.size()];
    //        Terms[] terms = new Terms[leaves.size()];
    //        for (int i=0; i<leaves.size(); i++) {
    //            AtomicReader reader = leaves.get(i).reader();
    //            readerSlices[i] = new ReaderSlice(0, reader.maxDoc(), i);
    //            terms[i] = new TermsFilteredByLiveDocs(reader.terms("key"), reader.getLiveDocs());
    //        }
    //        MultiTerms multiTerms = new MultiTerms(terms, readerSlices);
    //        final TermsEnum termsEnum = multiTerms.iterator(null);
    //
    //        return new PyIterator<String>() {
    //            @Override
    //            public String next() {
    //                try {
    //                    BytesRef term = null;
    //                    term = termsEnum.next();
    //                    if (term == null) {
    //                        return null;
    //                    }
    //                    return term.utf8ToString();
    //                } catch (IOException e) {
    //                    throw new RuntimeException(e);
    //                }
    //            }
    //        };
    //    }
    //
    //    public interface PyIterator<T> {
    //        public T next();
    //    }
    //
    //
    //    class TermsFilteredByLiveDocs extends FilterTerms {
    //        Bits liveDocs;
    //
    //        public TermsFilteredByLiveDocs(Terms terms, Bits liveDocs) {
    //            super(terms);
    //            this.liveDocs = liveDocs;
    //        }
    //
    //        @Override
    //        public TermsEnum iterator(TermsEnum original) throws IOException {
    //            final TermsEnum termsEnum = this.in.iterator(null);
    //
    //            return new FilteredTermsEnum(termsEnum, false) {
    //                @Override
    //                public AcceptStatus accept(BytesRef term) throws IOException {
    //                    DocsEnum docsEnum = termsEnum.docs(TermsFilteredByLiveDocs.this.liveDocs, null, DocsEnum.FLAG_NONE);
    //                    int docId = docsEnum.nextDoc();
    //                    if (docId == DocsEnum.NO_MORE_DOCS) {
    //                        return AcceptStatus.NO;
    //                    }
    //                    return AcceptStatus.YES;
    //                }
    //            };
    //        }
    //    }
}