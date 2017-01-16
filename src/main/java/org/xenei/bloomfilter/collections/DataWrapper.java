package org.xenei.bloomfilter.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.xenei.bloomfilter.BloomFilterI;

public class DataWrapper<T, F extends BloomFilterI> implements Comparable<DataWrapper<T, F>> {
	private List<T> data;
	private F filter;

	public DataWrapper(F f, T t) {
		this.filter = f;
		this.data = Arrays.asList(t);
	}

	public F getFilter() {
		return filter;
	}

	public Iterator<T> getData() {
		return data.iterator();
	}

	public int size() {
		return data.size();
	}

	public T get(int i) {
		return data.get(i);
	}

	public void add(T t) {
		if (data.size() == 1) {
			T t1 = data.get(0);
			data = new ArrayList<T>(2);
			data.add(t1);
		}
		data.add(t);
	}

	@Override
	public int hashCode() {
		return filter.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return (o instanceof DataWrapper<?, ?>) ? ((DataWrapper<?, ?>) o).filter.equals(filter) : false;
	}

	@Override
	public int compareTo(DataWrapper<T, F> o) {
		return filter.compareTo(o.filter);
	}

	@Override
	public String toString() {
		return String.format("DataWrapper[ %s x %s [%s]", data.size(), filter, data);
	}

}
