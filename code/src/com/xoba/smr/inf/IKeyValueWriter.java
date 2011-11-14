package com.xoba.smr.inf;

import java.io.OutputStream;

public interface IKeyValueWriter {

	public void write(OutputStream out, byte[] key, byte[] value) throws Exception;

}