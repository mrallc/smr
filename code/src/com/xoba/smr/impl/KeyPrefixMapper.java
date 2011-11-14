package com.xoba.smr.impl;

import com.xoba.smr.inf.IMapper;
import com.xoba.smr.inf.ICollector;

public final class KeyPrefixMapper implements IMapper {

	private final int p = 3;

	@Override
	public void map(byte[] key, byte[] value, ICollector output) throws Exception {
		int n = key.length;
		byte[] keyPrefix = new byte[p > n ? n : p];
		System.arraycopy(key, 0, keyPrefix, 0, keyPrefix.length);
		output.collect(keyPrefix, value);
	}

}