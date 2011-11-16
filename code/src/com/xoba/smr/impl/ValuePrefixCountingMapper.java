package com.xoba.smr.impl;

import com.xoba.smr.inf.ICollector;
import com.xoba.smr.inf.IMappingContext;

public final class ValuePrefixCountingMapper extends AbstractMapper {

	@Override
	public void map(byte[] key, byte[] value, ICollector output) throws Exception {
		byte[] valuePrefix = new byte[2];
		System.arraycopy(value, 0, valuePrefix, 0, valuePrefix.length);
		output.collect(valuePrefix, "1".getBytes());
	}

	@Override
	public void beginContext(IMappingContext context) {

	}

}