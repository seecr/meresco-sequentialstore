package org.meresco.sequentialstore;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;

class LuceneIndex {
	private DirectoryReader reader;
	private IndexWriter writer;
	private IndexSearcher searcher;

	LuceneIndex(String path, SortField sortField) throws IOException {
		Directory directory = FSDirectory.open(new File(path));
		IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, null);
		config.setRAMBufferSizeMB(256.0); // faster
		config.setUseCompoundFile(false); // faster, for Lucene 4.4 and later
		// Sort field must be indexed for sorting to work!
		config.setMergePolicy(new SortingMergePolicy(config.getMergePolicy(), new Sort(sortField)));
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
		if (this.reader != null) {
			try {
				this.reader.close();
			} catch (IOException e) {
			} finally {
				this.reader = null;
			}
		}
	}

	public void addDocument(Document doc) throws IOException {
		this.writer.addDocument(doc);
	}

	public int search_doc_id(Query query) throws IOException {
		TopDocs results = this.searcher.search(query, 1);
		if (results.totalHits > 0) {
			return results.scoreDocs[0].doc;
		}
		return -1;
	}

	public Document get_document(int docId) throws IOException {
		return this.reader.document(docId);
	}

	public int maxDoc() {
		return this.writer.maxDoc();
	}

	public void flush() throws IOException {
		this.writer.commit();
	}

	public String search_binary_value(Query q, final String field) throws IOException {
		final String result[] = new String[] { null };
		this.searcher.search(q, new Collector() {

			private BinaryDocValues data;

			@Override
			public void setScorer(Scorer scorer) throws IOException {
			}

			@Override
			public void collect(int doc) throws IOException {
				result[0] = data.get(doc).utf8ToString();
				throw new CollectionTerminatedException();
			}

			@Override
			public void setNextReader(AtomicReaderContext context) throws IOException {
				this.data = context.reader().getBinaryDocValues(field);
			}

			@Override
			public boolean acceptsDocsOutOfOrder() {
				return true;
			}
		});
		return result[0];
	}

	public FixedBitSet find_doc_ids(Query query, int maxValue) throws IOException {
		final FixedBitSet doc_ids = new FixedBitSet(reader.maxDoc());
		this.searcher.search(query, new Collector() {
			int base = 0;

			@Override
			public void setScorer(Scorer scorer) throws IOException {
			}

			@Override
			public void collect(int doc) throws IOException {
				doc_ids.set(base + doc);
			}

			@Override
			public void setNextReader(AtomicReaderContext context) throws IOException {
				this.base = context.docBase;
			}

			@Override
			public boolean acceptsDocsOutOfOrder() {
				return true;
			}
		});
		return doc_ids;
	}

	public String get_binary_value(int doc_id, String field) throws IOException {
		List<AtomicReaderContext> leaves = reader.leaves();
		int index = ReaderUtil.subIndex(doc_id, leaves);
		AtomicReaderContext leaf_reader_context = leaves.get(index);
		int local_doc_id = doc_id - leaf_reader_context.docBase;
		return leaf_reader_context.reader().getBinaryDocValues(field).get(local_doc_id).utf8ToString();
	}

	public void delete_all_but(final Bits current_keys) throws IOException {
		Query q = new MultiTermQuery("key") {

			@Override
			protected TermsEnum getTermsEnum(Terms terms, AttributeSource attrs) throws IOException {
				return new FilterKeys(terms.iterator(null), current_keys);
			}

			@Override
			public String toString(String arg0) {
				return "MultiTermQuery(key)";
			}
		};
		writer.deleteDocuments(q);
	}

	private final static class FilterKeys extends FilteredTermsEnum {
		private final Bits current_keys;

		private FilterKeys(TermsEnum termsEnum, Bits current_keys) {
			super(termsEnum, false);
			this.current_keys = current_keys;
		}

		@Override
		protected AcceptStatus accept(BytesRef term) throws IOException {
			return current_keys.get(NumericUtils.prefixCodedToInt(term)) ? AcceptStatus.NO : AcceptStatus.YES;
		}
	}
}