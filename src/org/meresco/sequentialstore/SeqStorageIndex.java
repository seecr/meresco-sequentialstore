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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.AtomicReader;

import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.lucene.index.sorter.NumericDocValuesSorter;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import org.apache.lucene.search.IndexSearcher;

import org.apache.lucene.util.Version;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;


public class SeqStorageIndex {
    private FieldType stampType;
    private IndexWriter writer;
    private DirectoryReader reader;
    public IndexSearcher searcher;

    public SeqStorageIndex(String path) throws IOException {
        this.stampType = new FieldType();
        this.stampType.setIndexed(true);
        this.stampType.setStored(false);
        this.stampType.setNumericType(FieldType.NumericType.LONG);

        Directory directory = FSDirectory.open(new File(path));
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, null);
        config.setRAMBufferSizeMB(256.0);  // faster
        //config.setUseCompoundFile(false);  // faster, for Lucene 4.4 and later
        MergePolicy mergePolicy = config.getMergePolicy();
        MergePolicy sortingMergePolicy = new SortingMergePolicy(mergePolicy, new NumericDocValuesSorter("value", true));
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

    public void close() {
        if (this.writer != null) {
            try {
                this.writer.close();
            } catch (IOException e) {
            } finally {
                this.writer = null;
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
            if (docId == docsEnum.NO_MORE_DOCS) {
                continue;
            }
            NumericDocValues numDocValues = reader.getNumericDocValues("value");
            return numDocValues.get(docId);
        }
        return -1;
    }

    public PyIterator<Long> itervalues() throws IOException {
        final Iterator<AtomicReaderContext> leaves = this.reader.leaves().iterator();  // TODO: sort segments by docBase!

        return new PyIterator<Long>() {
            AtomicReaderContext leaf = leaves.next();
            TermsEnum termsEnum = leaf.reader().terms("value").iterator(null);

            public Long next() {
                System.out.println("next called");
                try {
                    BytesRef term = termsEnum.next();
                    while (term == null) {
                        try {
                            System.out.println("next leaf");
                            leaf = leaves.next();
                        } catch (NoSuchElementException e) {
                            return null;  // communicate 'StopIteration' to python
                        }
                        termsEnum = leaf.reader().terms("value").iterator(null);
                        term = termsEnum.next();
                        // TODO: deleted?
                    }
                    Long result = NumericUtils.prefixCodedToLong(term);
                    System.out.println("result " + result);
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        /*
            Per segment, 'termEnum' van 'terms' opvragen.
            Segmenten sorteren op eerste term.
            Voor elke next een nieuwe term opleveren.
            TODO: skip deleted docs.
        */
    }


    public interface PyIterator<T> {
        public T next();
    }
}
