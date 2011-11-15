package com.xoba.smr.impl;

import java.util.Iterator;

import com.xoba.smr.inf.ICollector;
import com.xoba.smr.inf.IReducer;

public final class IdentityReducer implements IReducer {

	@Override
	public void reduce(byte[] key, Iterator<byte[]> values, ICollector output) throws Exception {
		while (values.hasNext()) {
			output.collect(key, values.next());
		}
	}

}