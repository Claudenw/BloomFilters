package org.xenei.bloomfilter;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Utils {

	public static <T> Stream<T> iteratorToStream(Iterator<T> iterator) {
		Iterable<T> iterable = () -> iterator;
		boolean parallel = false;
		return StreamSupport.stream(iterable.spliterator(), parallel);
	}

}
