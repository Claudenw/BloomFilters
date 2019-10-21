package org.xenei.bloomfilter.collections;

import java.io.IOException;
import java.util.function.Function;

import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

public interface BloomCollectionFactory<T> {

	BloomCollection<T> getCollection( FilterConfig config, Function<T, ProtoBloomFilter> func) throws IOException;
}
