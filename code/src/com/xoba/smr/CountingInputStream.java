package com.xoba.smr;

import java.io.IOException;
import java.io.InputStream;

/**
 * keeps count of how many bytes have been read
 * 
 */
public class CountingInputStream extends InputStream {

	private final InputStream in;
	private long count = 0;

	public CountingInputStream(InputStream i) {
		this.in = i;
	}

	public long getCount() {
		return count;
	}

	@Override
	public int read(byte[] b) throws IOException {
		int c = in.read(b);
		if (c > 0) {
			count += c;
		}
		return c;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int c = in.read(b, off, len);
		if (c > 0) {
			count += c;
		}
		return c;
	}

	@Override
	public long skip(long n) throws IOException {
		long c = in.skip(n);
		if (c > 0) {
			count += c;
		}
		return c;
	}

	@Override
	public int available() throws IOException {
		return in.available();
	}

	@Override
	public void close() throws IOException {
		in.close();
	}

	@Override
	public void mark(int readlimit) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void reset() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean markSupported() {
		return false;
	}

	@Override
	public int read() throws IOException {
		int c = in.read();
		if (c > 0) {
			count++;
		}
		return c;
	}

}