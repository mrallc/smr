package com.xoba.smr.impl;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.xoba.smr.inf.ICollector;
import com.xoba.smr.inf.IKeyValueReader;

public class AsciiTSVReader implements IKeyValueReader {

	@Override
	public void readFully(InputStream in, ICollector out) throws Exception {
		final byte[] empty = new byte[0];
		BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(in)));
		try {
			boolean done = false;
			while (!done) {
				String line = reader.readLine();
				if (line == null) {
					done = true;
				} else {
					String[] kv = line.split("\t");
					switch (kv.length) {
					case 0:
						out.collect(empty, empty);
						break;
					case 1:
						out.collect(kv[0].getBytes("US-ASCII"), empty);
						break;
					default:
						out.collect(kv[0].getBytes("US-ASCII"), kv[1].getBytes("US-ASCII"));
						break;
					}
				}
			}
		} finally {
			reader.close();
		}
	}
}