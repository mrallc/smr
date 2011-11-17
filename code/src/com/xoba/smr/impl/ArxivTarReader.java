package com.xoba.smr.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Formatter;

import org.xeustechnologies.jtar.TarEntry;
import org.xeustechnologies.jtar.TarInputStream;

import com.xoba.smr.inf.ICollector;
import com.xoba.smr.inf.IKeyValueReader;
import com.xoba.util.MraUtils;

public class ArxivTarReader implements IKeyValueReader {

	@Override
	public void readFully(InputStream in, ICollector out) throws Exception {

		TarInputStream tis = new TarInputStream(new BufferedInputStream(in));
		TarEntry entry = null;
		while ((entry = tis.getNextEntry()) != null) {

			InputStream nin = new NoClosingInput(tis);

			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			MraUtils.copy(nin, bout);
			bout.close();

			String key = new Formatter().format("%s", entry.getName()).toString();
			String value = new Formatter().format("%d", bout.toByteArray().length).toString();

			out.collect(key.getBytes(), value.getBytes());
		}

		tis.close();
	}

	private static final class NoClosingInput extends InputStream {

		private final TarInputStream tis;

		private NoClosingInput(TarInputStream tis) {
			this.tis = tis;
		}

		@Override
		public int available() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public synchronized void mark(int arg0) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean markSupported() {
			return false;
		}

		@Override
		public int read(byte[] arg0, int arg1, int arg2) throws IOException {
			return tis.read(arg0, arg1, arg2);
		}

		@Override
		public int read(byte[] arg0) throws IOException {
			return tis.read(arg0);
		}

		@Override
		public synchronized void reset() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public long skip(long arg0) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public int read() throws IOException {
			return tis.read();
		}
	}

}
