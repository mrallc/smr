package com.xoba.smr.impl;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.xoba.smr.inf.IKeyValueReader;
import com.xoba.smr.inf.ICollector;

public class AsciiTSVReader implements IKeyValueReader {

	@Override
	public void readFully(InputStream in, ICollector out) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		try {
			boolean done = false;
			while (!done) {
				String line = reader.readLine();
				if (line == null) {
					done = true;
				} else {
					String[] kv = line.split("\t");
					out.collect(kv[0].getBytes("US-ASCII"), kv[1].getBytes("US-ASCII"));
				}
			}
		} finally {
			reader.close();
		}
	}

}