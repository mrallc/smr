package com.xoba.smr.impl;

import com.xoba.smr.inf.IMapper;
import com.xoba.smr.inf.ICollector;

public final class ValuePrefixCountingMapper implements IMapper {

	@Override
	public void map(byte[] key, byte[] value, ICollector output) throws Exception {
		byte[] valuePrefix = new byte[2];
		System.arraycopy(value, 0, valuePrefix, 0, valuePrefix.length);
		output.collect(valuePrefix, "1".getBytes());
	}

}