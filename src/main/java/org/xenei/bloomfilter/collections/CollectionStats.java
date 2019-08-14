package org.xenei.bloomfilter.collections;

public class CollectionStats {

	private long filterInserts;
	private long filterDeletes;
	
	public CollectionStats() {
		filterInserts = 0;
		filterDeletes = 0;
	}
	
	public void clear() {
		filterInserts = 0;
		filterDeletes = 0;
	}
	
	public static int asInt( long l ) {
		return (l > Integer.MAX_VALUE ) ? Integer.MAX_VALUE : 
			( l < Integer.MIN_VALUE ) ? Integer.MIN_VALUE : (int)l;
	}

	public long getFilterCount() {
		return filterInserts - filterDeletes;
	}
	
	public double badHitFactor() {
		if (filterInserts == 0 || filterDeletes == 0)
		{
			return 0.0;
		}
		double ratio = filterDeletes / (filterInserts * 1.0); 
		return ratio;
	}
	
	public void insert() {
		if (filterInserts < Long.MAX_VALUE)
		{
			filterInserts++;
		}
	}
	
	public void delete() {
		delete( 1 );
	}
	
	public void delete( long count ) {
		if (filterDeletes < Long.MAX_VALUE-count)
		{
			filterDeletes+=count;
		}
		else {
			filterDeletes = Long.MAX_VALUE;
		}
	}

	public long getInsertCount() {
		return filterInserts;
	}

	public long getDeleteCount() {
		return filterDeletes;
	}
	
	public long getTxnCount() {
		return filterInserts + filterDeletes;
	}
	
	
	
}
