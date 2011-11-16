package com.xoba.smr.impl;

import com.xoba.smr.inf.ICollector;

public final class IdentityMapper extends AbstractMapper {

	@Override
	public void map(byte[] key, byte[] value, ICollector output) throws Exception {
		output.collect(key, value);
	}

}