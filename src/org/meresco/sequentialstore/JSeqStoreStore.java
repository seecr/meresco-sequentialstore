package org.meresco.sequentialstore;

import java.io.IOException;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

public class JSeqStoreStore {

	private LuceneIndex index;
	private Document doc = new Document();
	private IntField key_field;
	private BinaryDocValuesField data_field;

	public JSeqStoreStore(String path) throws IOException {
		this.index = new LuceneIndex(path, new SortField("key", SortField.Type.INT));
		FieldType keyType = new FieldType();
		keyType.setNumericType(FieldType.NumericType.INT);
		keyType.setNumericPrecisionStep(Integer.MAX_VALUE);
		keyType.setStored(true);
		keyType.setIndexed(true);
		keyType.setOmitNorms(true);
		keyType.setIndexOptions(IndexOptions.DOCS_ONLY);
		this.key_field = new IntField("key", -1, keyType);
		this.data_field = new BinaryDocValuesField("data", new BytesRef());
		doc.add(data_field);
		doc.add(key_field);
	}

	public void add(int key, String data) throws IOException {
		this.key_field.setIntValue(key);
		this.data_field.setBytesValue(new BytesRef(data));
		this.index.addDocument(doc);
	}

	public void delete_all_but(Bits current_keys) throws IOException {
		this.index.delete_all_but(current_keys);
	}

	public int lastKey() throws IOException {
		int maxDoc = this.index.maxDoc();
		if (maxDoc == 0)
			return 0;
		return this.index.get_document(maxDoc - 1).getField("key").numericValue().intValue();
	}

	public String get(int key) throws IOException {
		Query q = NumericRangeQuery.newIntRange("key", Integer.MAX_VALUE, key, key, true, true);
		String value = this.index.search_binary_value(q, "data");
		if (value == null)
			throw new RuntimeException("IndexError," + key);
		return value;
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

		public Event(int doc_id) throws IOException {
			this.data = index.get_binary_value(doc_id, "data");
			this.key = index.get_document(doc_id).getField("key").numericValue().intValue();
		}
	}

	public Iterator<Event> range(int start_key, int stop_key, boolean inclusive) throws IOException {
		Query q1 = NumericRangeQuery.newIntRange("key", Integer.MAX_VALUE, start_key, stop_key == -1 ? null : stop_key,
				true, inclusive);
		final FixedBitSet doc_ids = this.index.find_doc_ids(q1, Integer.MAX_VALUE);

		return new Iterator<Event>() {
			int doc_id = 0;

			public Event next() {
				doc_id = doc_ids.nextSetBit(doc_id);
				if (doc_id == -1)
					return null;
				try {
					return new Event(doc_id);
				} catch (IOException e) {
					throw new RuntimeException(e);
				} finally {
					doc_id++;
				}
			}
		};
	}

	public Iterator<Event> list_events() throws IOException {
		return this.range(0, Integer.MAX_VALUE, false);
	}
}
