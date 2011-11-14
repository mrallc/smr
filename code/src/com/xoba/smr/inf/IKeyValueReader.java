package com.xoba.smr.inf;

import java.io.InputStream;

public interface IKeyValueReader {

	public void readFully(InputStream in, ICollector out) throws Exception;

}