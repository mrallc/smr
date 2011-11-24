package com.xoba.smr.impl;

import com.xoba.smr.inf.IKeyValueComparator;
import com.xoba.util.MraUtils;

public class RawKVComparator implements IKeyValueComparator {

	@Override
	public int compare(byte[] key1, byte[] key2, byte[] value1, byte[] value2) {
		final int cmp = MraUtils.compareArrays(key1, key2);
		if (cmp == 0) {
			return MraUtils.compareArrays(value1, value2);
		} else {
			return cmp;
		}
	}

}
