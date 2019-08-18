package org.xenei.bloomfilter.collections.persist;

import java.nio.ByteBuffer;

import org.xenei.span.LongSpan;

public class Block implements Comparable<Block> {
	LongSpan pos;
	Block next;
	ByteBuffer buffer;

	Block(long start, int length) {
		pos = LongSpan.fromLength(start, length);
		next = null;
		buffer = ByteBuffer.allocate(length);
	}

	@Override
	public int compareTo(Block arg0) {
		return Long.compare(this.pos.getOffset(), arg0.pos.getOffset());
	}

}
