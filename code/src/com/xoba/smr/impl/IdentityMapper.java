package com.xoba.smr.impl;

import com.xoba.smr.inf.IMapper;
import com.xoba.smr.inf.ICollector;

public final class IdentityMapper implements IMapper {

	@Override
	public void map(byte[] key, byte[] value, ICollector output) throws Exception {
		output.collect(key, value);
	}

}