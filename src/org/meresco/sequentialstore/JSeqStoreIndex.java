/* begin license *
 *
 * "Meresco SequentialStore" are components to build Oai repositories, based on
 * "Meresco Core" and "Meresco Components".
 *
 * Copyright (C) 2013-2014 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 *
 * This file is part of "Meresco SequentialStore"
 *
 * "Meresco Oai" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Oai" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Oai"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

package org.meresco.sequentialstore;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FilterAtomicReader.FilterTerms;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;


public class JSeqStoreIndex {
    public FieldType stampType;
    public IndexWriter writer;
    public DirectoryReader reader;
    public IndexSearcher searcher;

    public JSeqStoreIndex(String path) throws IOException {
        this.stampType = new FieldType();
        this.stampType.setIndexed(true);
        this.stampType.setStored(false);
        this.stampType.setNumericType(FieldType.NumericType.LONG);
        this.stampType.setIndexOptions(IndexOptions.DOCS_ONLY);

        Directory directory = FSDirectory.open(new File(path));
        IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, null);
        config.setRAMBufferSizeMB(256.0);  // faster
        config.setUseCompoundFile(false);  // faster, for Lucene 4.4 and later
        MergePolicy mergePolicy = config.getMergePolicy();
        MergePolicy sortingMergePolicy = new SortingMergePolicy(mergePolicy, new Sort(new SortField("value", SortField.Type.LONG)));
        config.setMergePolicy(sortingMergePolicy);
        this.writer = new IndexWriter(directory, config);
        this.reader = DirectoryReader.open(this.writer, true);
        this.searcher = new IndexSearcher(this.reader);
    }

    public void reopen() throws IOException {
        DirectoryReader newReader = DirectoryReader.openIfChanged(this.reader, this.writer, true);
        if (newReader != null) {
            this.reader.close();
            this.reader = newReader;
            this.searcher = new IndexSearcher(this.reader);
        }
    }

    public int length() {
        // Needs (writer.)commit()
        return this.writer.numDocs();
    }

    public void commit() throws IOException {
        if (this.writer != null) {
            this.writer.commit();
        }
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
            }
        }
    }

    public void setKeyValue(String key, long value) throws IOException {
        Document doc = new Document();
        StringField keyField = new StringField("key", key, Field.Store.NO);
        doc.add(keyField);
        LongField valueField = new LongField("value", value, this.stampType);
        doc.add(valueField);
        doc.add(new NumericDocValuesField("value", value));
        this.writer.updateDocument(new Term("key", key), doc);
    }

    public void delete(String key) throws IOException {
        this.writer.deleteDocuments(new Term("key", key));
    }

    public long getValue(String key) throws IOException {
        List<AtomicReaderContext> leaves = this.reader.leaves();
        for (AtomicReaderContext leaf: leaves) {
            AtomicReader reader = leaf.reader();
            DocsEnum docsEnum = reader.termDocsEnum(new Term("key", new BytesRef(key)));
            if (docsEnum == null) {
                continue;
            }
            int docId = docsEnum.nextDoc();
            if (docId == DocsEnum.NO_MORE_DOCS) {
                continue;
            }
            NumericDocValues numDocValues = reader.getNumericDocValues("value");
            return numDocValues.get(docId);
        }
        return -1;
    }

    public PyIterator<String> iterkeys() throws IOException {
        // Needs this.reopen()
        List<AtomicReaderContext> leaves = this.reader.leaves();
        ReaderSlice[] readerSlices = new ReaderSlice[leaves.size()];
        Terms[] terms = new Terms[leaves.size()];
        for (int i=0; i<leaves.size(); i++) {
            AtomicReader reader = leaves.get(i).reader();
            readerSlices[i] = new ReaderSlice(0, reader.maxDoc(), i);
            terms[i] = new TermsFilteredByLiveDocs(reader.terms("key"), reader.getLiveDocs());
        }
        MultiTerms multiTerms = new MultiTerms(terms, readerSlices);
        final TermsEnum termsEnum = multiTerms.iterator(null);

        return new PyIterator<String>() {
            public String next() {
                try {
                    BytesRef term = null;
                    term = termsEnum.next();
                    if (term == null) {
                        return null;
                    }
                    return term.utf8ToString();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public PyIterator<Long> itervalues() throws IOException {
        // Needs this.reopen()
        List<AtomicReaderContext> leaves = this.reader.leaves();
        ReaderSlice[] readerSlices = new ReaderSlice[leaves.size()];
        Terms[] terms = new Terms[leaves.size()];
        for (int i=0; i<leaves.size(); i++) {
            AtomicReader reader = leaves.get(i).reader();
            readerSlices[i] = new ReaderSlice(0, reader.maxDoc(), i);
            terms[i] = new TermsFilteredByLiveDocs(reader.terms("value"), reader.getLiveDocs());
        }
        MultiTerms multiTerms = new MultiTerms(terms, readerSlices);
        final TermsEnum termsEnum = NumericUtils.filterPrefixCodedLongs(multiTerms.iterator(null));

        return new PyIterator<Long>() {
            public Long next() {
                try {
                    BytesRef term = null;
                    term = termsEnum.next();
                    if (term == null) {
                        return null;
                    }
                    return NumericUtils.prefixCodedToLong(term);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public interface PyIterator<T> {
        public T next();
    }


    class TermsFilteredByLiveDocs extends FilterTerms {
        Bits liveDocs;

        public TermsFilteredByLiveDocs(Terms terms, Bits liveDocs) {
            super(terms);
            this.liveDocs = liveDocs;
        }

        @Override
        public TermsEnum iterator(TermsEnum original) throws IOException {
            final TermsEnum termsEnum = this.in.iterator(null);

            return new FilteredTermsEnum(termsEnum, false) {
                @Override
                public AcceptStatus accept(BytesRef term) throws IOException {
                    DocsEnum docsEnum = termsEnum.docs(TermsFilteredByLiveDocs.this.liveDocs, null, DocsEnum.FLAG_NONE);
                    int docId = docsEnum.nextDoc();
                    if (docId == DocsEnum.NO_MORE_DOCS) {
                        return AcceptStatus.NO;
                    }
                    return AcceptStatus.YES;
                }
            };
        }
    }
}
