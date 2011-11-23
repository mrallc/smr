package com.xoba.smr.impl;

import java.io.OutputStream;

import com.xoba.smr.inf.IKeyValueWriter;

public class AsciiTSVWriter implements IKeyValueWriter {

	@Override
	public void write(final OutputStream out, final byte[] key, final byte[] value) throws Exception {
		out.write(key);
		out.write('\t');
		out.write(value);
		out.write('\n');
	}

}