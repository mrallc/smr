package com.xoba.smr.impl;

import com.xoba.smr.inf.IKeyValueComparator;

public class StringsKVComparator implements IKeyValueComparator {

	@Override
	public int compare(byte[] key1, byte[] key2, byte[] value1, byte[] value2) {
		int cmp = new String(key1).compareTo(new String(key2));
		if (cmp == 0) {
			return new String(value1).compareTo(new String(value2));
		} else {
			return cmp;
		}
	}

}
