package com.xoba.smr.impl;

import java.util.Iterator;

import com.xoba.smr.inf.ICollector;
import com.xoba.smr.inf.IReducer;

public final class ValueSummingReducer implements IReducer {

	@Override
	public void reduce(byte[] key, Iterator<byte[]> values, ICollector output) throws Exception {
		long count = 0;
		while (values.hasNext()) {
			count += new Long(new String(values.next()));
		}
		output.collect(key, ("" + count).getBytes());
	}

}