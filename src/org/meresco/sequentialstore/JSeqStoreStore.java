package org.meresco.sequentialstore;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

class LuceneIndex {
	DirectoryReader reader;
	IndexWriter writer;
	public IndexSearcher searcher;

	LuceneIndex(String path) throws IOException {
		Directory directory = FSDirectory.open(new File(path));
		IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, null);
		config.setRAMBufferSizeMB(256.0); // faster
		config.setUseCompoundFile(false); // faster, for Lucene 4.4 and later
		MergePolicy mergePolicy = config.getMergePolicy();
		MergePolicy sortingMergePolicy = new SortingMergePolicy(mergePolicy,
				new Sort(new SortField("value", SortField.Type.LONG)));
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

	public int find_docId(Query query) throws IOException {
		TopDocs results = this.searcher.search(query, 1);
		if (results.totalHits > 0) {
			return results.scoreDocs[0].doc;
		}
		return -1;
	}

	public Document search(Query query) throws IOException {
		int docid = this.find_docId(query);
		if (docid >= 0)
			return this.get_document(docid);
		return null;
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
};

public class JSeqStoreStore {

	private LuceneIndex index;

	public int lastKey() throws IOException {
		int maxDoc = this.index.maxDoc();
		if (maxDoc == 0)
			return 0;
		return this.index.get_document(maxDoc - 1).getField("key").numericValue().intValue();
	}

	public JSeqStoreStore(String path) throws IOException {
		this.index = new LuceneIndex(path);
	}

	public void add(int key, String data) throws IOException {
		Document doc = new Document();
		// doc.add(new StringField("key", "" + key, Store.YES));
		doc.add(new StoredField("data", data));
		doc.add(new IntField("key", key, Store.YES));
		this.index.addDocument(doc);
	}

	public String get(int key) throws IOException {
		Query q = NumericRangeQuery.newIntRange("key", key, key, true, true);
		Document doc = this.index.search(q);
		if (doc == null)
			throw new RuntimeException("IndexError," + key);
		return doc.get("data");
	}

	public void reopen() throws IOException {
		this.index.reopen();
	}

	public void flush() throws IOException {
		this.index.flush();
	}

	public void close() {
		this.index.close();
	}

	public Iterator<String> getMultiple(final int[] keys, final boolean ignore_missing) throws IOException {
		return new Iterator<String>() {
			int i = -1;
			int last_key = -1;

			@Override
			public String next() {
				try {
					String result = null;
					int key = -1;
					while (++i < keys.length) {
						key = keys[i];
						if (key <= last_key)
							throw new RuntimeException("Keys should have been sorted.");
						last_key = key;
						try {
							result = JSeqStoreStore.this.get(key);
						} catch (RuntimeException e) {
							if (ignore_missing)
								continue;
							else
								throw e;
						}
						break;
					}
					if (result != null)
						return "" + key + "\n" + result;
					else
						return null;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}

	public interface Iterator<T> {
		public T next();
	}

	public class Event {
		public String data;
		public int key;

		public Event(Document doc) {
			this.data = doc.get("data");
			this.key = doc.getField("key").numericValue().intValue();
		}
	}

	private Iterator<Event> range_events(final int start_doc, final int stop_doc) throws IOException {
		return new Iterator<Event>() {
			int docId = start_doc;

			public Event next() {
				if (docId >= stop_doc)
					return null;
				try {
					return new Event(JSeqStoreStore.this.index.get_document(docId));
				} catch (IOException e) {
					throw new RuntimeException(e);
				} finally {
					docId++;
				}
			}
		};

	}

	public Iterator<Event> range(int start_key, int stop_key, boolean inclusive) throws IOException {
		Query q1 = NumericRangeQuery.newIntRange("key", start_key == -1 ? null : start_key, null, true, false);
		int start_doc = this.index.find_docId(q1);
		int stop_doc = this.index.maxDoc();
		if (stop_key != -1) {
			Query q2 = NumericRangeQuery.newIntRange("key", stop_key, null, !inclusive, false);
			int stop = this.index.find_docId(q2);
			if (stop >= 0)
				stop_doc = stop;
		}
		return this.range_events(start_doc, stop_doc);
	}

	public Iterator<Event> list_events() throws IOException {
		return this.range_events(0, this.index.maxDoc());
	}
}
