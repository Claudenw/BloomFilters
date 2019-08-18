package org.xenei.bloomfilter.collections.storage;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.xenei.bloomfilter.ProtoBloomFilter;
import org.xenei.bloomfilter.collections.AbstractDataWrapper;
import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.streams.SpanBufferInputStream;

/**
 * A list of objects that share the same proto filter and are placed in storage.
 * 
 * @param <T>
 */
class Bucket<T> extends AbstractDataWrapper<T> {
	int objCount;
	SpanBuffer data;

	public Bucket(ProtoBloomFilter proto) {
		super(proto);
		data = Factory.EMPTY;
	}

	@Override
	public Iterator<T> getData() {
		return new BucketIterator();
	}

	@Override
	public int size() {
		return objCount;
	}

	@Override
	public void add(T t) {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();

				ObjectOutputStream oos = new ObjectOutputStream(baos);) {
			oos.writeObject(t);
			oos.flush();
			SpanBuffer sb = Factory.wrap(baos.toByteArray());
			data = Factory.merge(data, sb);
			objCount++;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public boolean remove(T t) {
		try (SpanBufferInputStream sbis = data.getInputStream()) {
			List<LongSpan> spansToRemove = new ArrayList<LongSpan>();
			ObjectInputStream ois = new ObjectInputStream(sbis);
			try {
				while (true) {
					long start = sbis.getBytesRead();
					Object other = ois.readObject();
					if (t.equals(other)) {
						spansToRemove.add(LongSpan.fromEnd(start, sbis.getBytesRead()));
					}
				}
			} catch (EOFException expected) {
				if (spansToRemove.size() == 0) {
					return false;
				}
				objCount -= spansToRemove.size();
				if (objCount <= 0) {
					objCount = 0;
					data = Factory.EMPTY;
				} else {
					List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
					long nxtStart = 0;
					for (LongSpan s : spansToRemove) {
						if (buffers.isEmpty()) {
							buffers.add(data.head(s.getOffset()));
						} else {
							buffers.add(data.tail(nxtStart).head(s.getOffset()));
						}
						nxtStart = s.getOffset() + s.getLength();
					}
					if (nxtStart < data.getLength()) {
						buffers.add(data.safeTail(nxtStart));
					}
					data = Factory.merge(buffers.iterator());
				}
				return true;
			}

		} catch (IOException | ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	private class BucketIterator implements Iterator<T> {
		private int next;
		private ObjectInputStream ois;

		BucketIterator() {
			next = 0;
			ois = null;
		}

		@Override
		public boolean hasNext() {
			return next < objCount;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T next() {
			if (hasNext()) {
				next++;
				if (ois == null) {
					try {
						ois = new ObjectInputStream(data.getInputStream());
					} catch (IOException e) {
						throw new IllegalStateException(e);
					}

				}
				try {
					return (T) ois.readObject();
				} catch (ClassNotFoundException | IOException e) {
					throw new IllegalStateException(e);
				}

			} else {
				throw new NoSuchElementException();
			}
		}

	}

}