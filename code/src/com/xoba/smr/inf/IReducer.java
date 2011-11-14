package com.xoba.smr.inf;

import java.util.Iterator;

public interface IReducer {

	public void reduce(byte[] key, Iterator<byte[]> values, ICollector output) throws Exception;

}
