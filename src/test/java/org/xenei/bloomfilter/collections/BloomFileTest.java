package org.xenei.bloomfilter.collections;

import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.ProtoBloomFilterBuilder;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;
import org.xenei.bloomfilter.collections.BloomList;

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

public class BloomFileTest {

	private BloomFile<String> list;
	private FilterConfig filterConfig = new FilterConfig(5, 10);
	private ProtoBloomFilterBuilder builder = new ProtoBloomFilterBuilder();

	private static Function<String, ProtoBloomFilter> func = new Function<String, ProtoBloomFilter>() {

		@Override
		public ProtoBloomFilter apply(String string) {
			return new ProtoBloomFilterBuilder().build(string);
		}

	};

	@Before
	public void before() throws IOException {
		File f = new File("/tmp/bf.tst");
		f.delete();
		list = BloomFile.create( "/tmp/bf.tst", filterConfig, func);
	}

	private void add(String s) throws IOException {
		ProtoBloomFilter pbf = builder.update(s).build();
		list.add(pbf, s);
	}

	@Test
	public void testWriteAndRead() throws Exception {
		String[] str = { "one", "two", "three", "four", "five" };
		for (String s : str) {
			add(s);
		}
		try (DataOutputStream oos = new DataOutputStream( new FileOutputStream( "/tmp/bf.tst" ))) {
			list.write(oos);
		}
		list = BloomFile.create( "/tmp/bf.tst", filterConfig, func);
		List<String> lst = list.stream().collect( Collectors.toList());
		for (String s : str) {
			assertTrue("missing " + s, lst.contains(s));
		}
		assertTrue(list.isFull());
	}
	
	@Test
	public void isFullTest() throws IOException {
		add("one");

		add("two");

		add("three");

		add("four");

		assertFalse("List should not be full", list.isFull());

		add("five");

		assertTrue("List should be full", list.isFull());

		add("six");

		assertTrue("List should still be full", list.isFull());
	}

	@Test
	public void addTest() throws IOException {
		String[] str = { "one", "two", "three", "four", "five" };
		for (String s : str) {
			add(s);
		}

		List<String> lst = list.stream().collect( Collectors.toList());
		for (String s : str) {
			assertTrue("missing " + s, lst.contains(s));
		}

		assertTrue(list.isFull());
		add("six");
		lst = list.stream().collect( Collectors.toList());
		for (String s : str) {
			assertTrue("missing " + s, lst.contains(s));
		}
		assertTrue("missing six", lst.contains("six"));
		assertEquals(6, lst.size());
		assertEquals(6, list.size());

		add("six");
		lst = list.stream().collect( Collectors.toList());
		// number of items returned
		assertEquals(7, lst.size());
	}

	@Test
	public void clearTest() throws IOException {
		String[] str = { "one", "two", "three", "four", "five" };
		for (String s : str) {
			add(s);
		}
		list.clear();

		assertFalse("Should not be any candidates", list.iterator().hasNext());
		assertFalse("Should not be full", list.isFull());

		for (String s : str) {
			assertFalse("should not contain " + s, list.matches(builder.update(s).build()));
		}
		assertEquals("List should be empty", 0, list.size());

	}

	@Test
	public void matchesTest() throws IOException {
		String[] str = { "one", "two", "three", "four", "five" };
		ProtoBloomFilter filters[] = new ProtoBloomFilter[str.length];
		BloomFilter bf[] = new BloomFilter[str.length];
		for (int i = 0; i < str.length; i++) {
			filters[i] = builder.update(str[i]).build();
			bf[i] = filters[i].create(filterConfig);
		}
		for (int i = 0; i < str.length; i++) {
			for (int j = 0; j < str.length; j++) {
				if (j < i) {
					assertTrue("gate should contain " + j + " " + filters[j], list.getGate().inverseMatch(bf[j]));
					assertTrue("list should contain " + j + " " + str[j], list.matches(filters[j]));
				} else {
					assertFalse("gate should not contain " + j + " " + filters[j], list.getGate().inverseMatch(bf[j]));
					assertFalse("list should not contain " + j + " " + str[j], list.matches(filters[j]));

				}
			}
			add(str[i]);
			for (int j = 0; j < str.length; j++) {
				if (j <= i) {
					assertTrue("gate should contain " + j + " " + filters[j], list.getGate().inverseMatch(bf[j]));
					assertTrue("list should contain " + j + " " + str[j], list.matches(filters[j]));
				} else {
					assertFalse("gate should not contain " + j + " " + filters[j], list.getGate().inverseMatch(bf[j]));
					assertFalse("list should not contain " + j + " " + str[j], list.matches(filters[j]));

				}
			}

		}

	}

	@Test
	public void distanceTest() throws IOException {
		ProtoBloomFilter pbf = builder.update("one").build();
		BloomFilter bf = pbf.create(filterConfig);

		assertEquals(bf.getHammingWeight(), list.distance(pbf));
		assertEquals(bf.getHammingWeight(), list.distance(bf));

		add("one");
		assertEquals(0, list.distance(pbf));
		assertEquals(0, list.distance(bf));

		add("two");
		assertEquals(3, list.distance(pbf));
		assertEquals(3, list.distance(bf));

		// test adding duplicate
		add("two");
		assertEquals(3, list.distance(pbf));
		assertEquals(3, list.distance(bf));
	}

	@Test
	public void candidatesTest() throws IOException {
		String[] str = { "one", "two" };
		for (String s : str) {
			add(s);
		}
		add("two");

		List<String> lst = list.stream().collect( Collectors.toList());
		assertEquals("Should be 3 candidates", 3, lst.size());

		lst = list.getCandidates(builder.update("two").build()).toList();
		assertEquals("Should be 2 candidates", 2, lst.size());

		lst = list.getCandidates(builder.update("three").build()).toList();
		assertEquals("Should be no candidates", 0, lst.size());

	}

	@Test
	public void hasExactMatchTest() throws IOException {
		ProtoBloomFilter pbf = builder.update("one").build();

		assertFalse("Should not have one", list.getExactMatches(pbf).hasNext());

		add("one");
		assertTrue("Should have one", list.getExactMatches(pbf).hasNext());

		pbf = builder.update("two").build();
		assertFalse("Should not have two", list.getExactMatches(pbf).hasNext());
	}

}
