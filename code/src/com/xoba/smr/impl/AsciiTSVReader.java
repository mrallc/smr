package com.xoba.smr.impl;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.xoba.smr.inf.ICollector;
import com.xoba.smr.inf.IKeyValueReader;
import com.xoba.util.ILogger;
import com.xoba.util.LogFactory;

/**
 * splits key/value at first tab character
 * 
 * @author mra
 * 
 */
public class AsciiTSVReader implements IKeyValueReader {

	private static final ILogger logger = LogFactory.getDefault().create();

	@Override
	public void readFully(final InputStream in, final ICollector out) throws Exception {
		final byte[] empty = new byte[0];
		final BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(in)));
		try {
			boolean done = false;
			while (!done) {
				final String line = reader.readLine();
				if (line == null) {
					done = true;
				} else {
					final String[] kv = split(line);
					switch (kv.length) {
					case 0:
						out.collect(empty, empty);
						break;
					case 1:
						out.collect(kv[0].getBytes("US-ASCII"), empty);
						break;
					case 2:
						out.collect(kv[0].getBytes("US-ASCII"), kv[1].getBytes("US-ASCII"));
						break;
					default:
						throw new IllegalStateException();
					}
				}
			}
		} finally {
			reader.close();
		}
	}

	private static String[] split(final String line) {
		final int sep = line.indexOf('\t');
		if (sep == -1) {
			return new String[] { line };
		} else {
			return new String[] { line.substring(0, sep), line.substring(sep + 1, line.length()) };
		}
	}

}