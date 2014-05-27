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

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.sorter.EarlyTerminatingSortingCollector;
import org.apache.lucene.index.sorter.NumericDocValuesSorter;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;


public class SeqStoreSortingCollector extends Collector {
    private int hitCount = 0;
    private boolean shouldCountHits;
    private int maxDocsToCollect;
    private int docBase;
    private boolean segmentCollectTerminated = false;
    private NumericDocValues segmentNumericDocValues;
    private long[] collectedValues;
    private int numberOfCollectedValues = 0;
    private long[] segmentValues;
    private int insertPosition;

    public SeqStoreSortingCollector(int maxDocsToCollect) throws IOException {
        this(maxDocsToCollect, false);
    }

    public SeqStoreSortingCollector(int maxDocsToCollect, boolean shouldCountHits) throws IOException {
        //System.out.println("new SeqStoreSortingCollector");
        this.maxDocsToCollect = maxDocsToCollect;
        this.shouldCountHits = shouldCountHits;
        this.collectedValues = new long[maxDocsToCollect * 2];
    }

    public long[] collectedValues() {
        collectSegmentValues();
        return this.collectedValues;  // TODO: maybe without trail of 0s
    }

    public int remainingRecords() {
        if (this.shouldCountHits) {
            return Math.max(0, this.hitCount - this.maxDocsToCollect);
        }
        return -1;
    }

    public int totalHits() {
        //System.out.println("totalHits" + this.hitCount);
        return this.hitCount;
    }

    @Override
    public void collect(int doc) throws IOException {
        //System.out.println("collect " + (this.docBase + doc) + " in segment with docBase " + this.docBase);
        this.hitCount++;
        if (segmentCollectTerminated) {
            return;
        }
        long value;
        if (this.insertPosition >= this.maxDocsToCollect) {
            segmentCollectTerminated = true;
        }
        else {
            value = this.segmentNumericDocValues.get(doc);
            if (this.numberOfCollectedValues >= this.maxDocsToCollect && value > this.collectedValues[this.maxDocsToCollect -1]) {
                //System.out.println("Skipping remainder of this segment as " + value + " > " + this.collectedValues[this.maxDocsToCollect -1]);
                segmentCollectTerminated = true;
            }
            else {
                this.segmentValues[this.insertPosition] = value;
                this.insertPosition++;
            }
        }
        if (segmentCollectTerminated) {
            if (!this.shouldCountHits) {
                throw new CollectionTerminatedException();
            }
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        collectSegmentValues();
        this.segmentCollectTerminated = false;
        this.docBase = context.docBase;
        this.segmentNumericDocValues = context.reader().getNumericDocValues("value");
        this.segmentValues = new long[maxDocsToCollect];
    }

    private void collectSegmentValues() {
        if (this.insertPosition > 0) {
            //System.out.println("collectSegmentValues: " + Arrays.toString(this.segmentValues) + ", " + Arrays.toString(this.collectedValues));
            long startTime = System.nanoTime();

            System.arraycopy(this.segmentValues, 0, this.collectedValues, numberOfCollectedValues, this.insertPosition);
            this.numberOfCollectedValues += this.insertPosition;
            Arrays.sort(this.collectedValues, 0, this.numberOfCollectedValues);
            Arrays.fill(this.collectedValues, this.maxDocsToCollect, this.maxDocsToCollect * 2, 0);
            this.numberOfCollectedValues = Math.min(this.numberOfCollectedValues, this.maxDocsToCollect);

            //System.out.println("collectSegmentValues took " + (System.nanoTime() - startTime));

            this.insertPosition = 0;
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }
}
