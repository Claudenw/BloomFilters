package org.xenei.bloomfilter.collections;

import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.BloomFilterBuilder;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;
import org.xenei.bloomfilter.collections.BloomList;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class BloomListTest {

	private BloomList<String> list;
	private FilterConfig filterConfig = new FilterConfig(5, 10);
	private BloomFilterBuilder builder = new BloomFilterBuilder();

	@Before
	public void before() {
		list = new BloomList<String>(filterConfig);
	}

	private void add(String s) throws IOException {
		ProtoBloomFilter pbf = builder.update(s).build();
		list.add(pbf, s);
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

		List<String> lst = list.getCandidates().toList();
		for (String s : str) {
			assertTrue("missing " + s, lst.contains(s));
		}

		assertTrue(list.isFull());
		add("six");
		lst = list.getCandidates().toList();
		for (String s : str) {
			assertTrue("missing " + s, lst.contains(s));
		}
		assertTrue("missing six", lst.contains("six"));
		assertEquals(6, lst.size());
		assertEquals(6, list.size());

		add("six");
		lst = list.getCandidates().toList();
		// number of items returned
		assertEquals(7, lst.size());
		// number of hashes in the list.
		assertEquals(6, list.size());

	}

	@Test
	public void clearTest() throws IOException {
		String[] str = { "one", "two", "three", "four", "five" };
		for (String s : str) {
			add(s);
		}
		list.clear();

		assertFalse("Should not be any candidates", list.getCandidates().hasNext());
		assertFalse("Shold not be full", list.isFull());

		for (String s : str) {
			assertFalse("should not contain " + s, list.contains(builder.update(s).build()));
		}
		assertEquals("List should be empty", 0, list.size());

	}

	@Test
	public void containsTest() throws IOException {
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
					assertTrue("gate should contain " + j + " " + filters[j], list.getGate().inverseMatch(filters[j]));
					assertTrue("list should contain " + j + " " + str[j], list.contains(filters[j]));
				} else {
					assertFalse("gate should not contain " + j + " " + filters[j],
							list.getGate().inverseMatch(filters[j]));
					assertFalse("list should not contain " + j + " " + str[j], list.contains(filters[j]));

				}
			}
			add(str[i]);
			for (int j = 0; j < str.length; j++) {
				if (j <= i) {
					assertTrue("gate should contain " + j + " " + filters[j], list.getGate().inverseMatch(filters[j]));
					assertTrue("list should contain " + j + " " + str[j], list.contains(filters[j]));
				} else {
					assertFalse("gate should not contain " + j + " " + filters[j],
							list.getGate().inverseMatch(filters[j]));
					assertFalse("list should not contain " + j + " " + str[j], list.contains(filters[j]));

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

		List<String> lst = list.getCandidates().toList();
		assertEquals("Should be 3 candidates", 3, lst.size());

		lst = list.getCandidates(builder.update("two").build()).toList();
		assertEquals("Should be 2 candidates", 2, lst.size());

		lst = list.getCandidates(builder.update("three").build()).toList();
		assertEquals("Should be no candidates", 0, lst.size());

	}

	@Test
	public void hasExactMatchTest() throws IOException {
		ProtoBloomFilter pbf = builder.update("one").build();

		assertFalse("Should not have one", list.hasExactMatch(pbf));

		add("one");
		assertTrue("Should have one", list.hasExactMatch(pbf));

		pbf = builder.update("two").build();
		assertFalse("Should not have two", list.hasExactMatch(pbf));
	}

}
