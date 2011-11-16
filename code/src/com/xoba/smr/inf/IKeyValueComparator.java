package com.xoba.smr.inf;

public interface IKeyValueComparator {

	/**
	 * must sort primarily on keys, optionally secondarily on values
	 * 
	 * @return negative if kv1 less than kv2; 0 if kv1 equals to kv2; positive
	 *         if kv1 greater than kv2
	 */
	public int compare(byte[] key1, byte[] key2, byte[] value1, byte[] value2);

}
