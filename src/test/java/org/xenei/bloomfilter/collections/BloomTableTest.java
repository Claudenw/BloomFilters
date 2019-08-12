package org.xenei.bloomfilter.collections;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.xenei.bloomfilter.ProtoBloomFilterBuilder;
import org.xenei.bloomfilter.ProtoBloomFilter;
import org.xenei.bloomfilter.collections.BloomTable;

public class BloomTableTest {
	private BloomTable<String> list;

	private static Function<String,ProtoBloomFilter> func = new Function<String,ProtoBloomFilter>()
	{

		@Override
		public ProtoBloomFilter apply(String string) {
			return new ProtoBloomFilterBuilder().build(string);
		}

	};
	
	@Before
	public void before() {
		list = new BloomTable<String>(func);
	}

	@Test
	public void putCollisionTest() throws IOException {

		String[] str = { "one", "two", "three", "four", "five" };
		String collision = "g"; // collides with "five"

		for (String s : str) {
			list.put(s);
		}

		list.put(collision);

		List<String> lst = list.getCandidates(collision).toList();
		assertEquals(1, lst.size());
		assertFalse("found five", lst.contains("five"));
		assertTrue("missing g", lst.contains("g"));

		lst = list.getCandidates("five").toList();
		assertEquals(1, lst.size());
		assertTrue("missing five", lst.contains("five"));
		assertFalse("found g", lst.contains("g"));

		lst = list.getCandidates("one").toList();
		assertEquals(1, lst.size());
		assertTrue("missing one", lst.contains("one"));
	}

	@Test
	public void putTest() {
		String[] str = { "one", "two", "three", "four", "five" };
		int[] values = { 1, 1, 1, 1, 1 };
		for (String s : str) {
			list.put(s);
		}
		List<String> lst = null;
		int idx = 0;
		for (String s : str) {
			lst = list.getCandidates(s).toList();
			assertEquals("Testing " + s, values[idx++], lst.size());
		}

		lst = list.getCandidates("six").toList();
		assertEquals("Should be none", 0, lst.size());

		list.put("five");

		for (String s : str) {
			lst = list.getCandidates(s).toList();
			if (s.equals("five")) {
				assertEquals("Should be two", 2, lst.size());

			} else {
				assertEquals("Should be one", 1, lst.size());
			}
		}
	}
}
