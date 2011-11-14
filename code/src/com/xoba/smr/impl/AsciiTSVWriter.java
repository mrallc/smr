package com.xoba.smr.impl;

import java.io.OutputStream;

import com.xoba.smr.inf.IKeyValueWriter;

public class AsciiTSVWriter implements IKeyValueWriter {

	@Override
	public void write(OutputStream out, byte[] key, byte[] value) throws Exception {
		out.write(key);
		out.write('\t');
		out.write(value);
		out.write('\n');
	}

}