package com.xoba.smr.impl;

import com.xoba.smr.inf.IKeyValueComparator;
import com.xoba.util.MraUtils;

public class RawKeyComparator implements IKeyValueComparator {

	@Override
	public int compare(byte[] key1, byte[] key2, byte[] value1, byte[] value2) {
		return MraUtils.compareArrays(key1, key2);
	}

}
