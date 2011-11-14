package com.xoba.smr.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.xeustechnologies.jtar.TarEntry;
import org.xeustechnologies.jtar.TarInputStream;

import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.handlers.AbstractRequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.xoba.amazonaws.AWSUtils;
import com.xoba.amazonaws.AWSUtils.IBucketListener;
import com.xoba.smr.SimpleMapReduce;
import com.xoba.smr.inf.ICollector;
import com.xoba.smr.inf.IKeyValueReader;
import com.xoba.util.ILogger;
import com.xoba.util.LogFactory;
import com.xoba.util.MraUtils;

public class ArxivTarReader implements IKeyValueReader {

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

	private static final ILogger logger = LogFactory.getDefault().create();

	public static void main(String[] args) throws Exception {

		if (true) {
			InputStream in = new BufferedInputStream(new FileInputStream(new File("/tmp/arxiv/arXiv_src_9902_001.tar")));
			IKeyValueReader reader = new ArxivTarReader();

			reader.readFully(in, new ICollector() {
				@Override
				public void collect(byte[] key, byte[] value) throws Exception {
					logger.debugf("%s: %s", new String(key), new String(value));
				}
			});

			System.exit(0);
		}

		AWSCredentials aws = SimpleMapReduce.createCreds();
		AmazonS3 s3 = new AmazonS3Client(aws);

		s3.addRequestHandler(new AbstractRequestHandler() {
			@Override
			public void beforeRequest(Request<?> request) {
				request.addHeader("x-amz-request-payer", "requester");
			}
		});

		File target = new File("/tmp/arXiv_src_9902_001.tar");
		if (!target.exists()) {
			File tmp = new File(target.getParentFile(), UUID.randomUUID().toString());
			try {
				s3.getObject(new GetObjectRequest("arxiv", "src/arXiv_src_9902_001.tar"), tmp);
				tmp.renameTo(target);
			} finally {
				tmp.delete();
			}
		}

		logger.debugf("got %,d bytes", target.length());

		TarInputStream tis = new TarInputStream(new BufferedInputStream(new FileInputStream(target)));
		TarEntry entry;
		while ((entry = tis.getNextEntry()) != null) {

			InputStream in = new NoClosingInput(tis);

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			MraUtils.copy(in, out);
			out.close();

			logger.debugf("wrote %,8d bytes from %s", out.toByteArray().length, entry.getName());
		}

		tis.close();

		System.exit(0);

		final Set<String> keys = new HashSet<String>();

		AWSUtils.scanObjectsInBucket(s3, "arxiv", new IBucketListener() {

			@Override
			public void done() {

			}

			@Override
			public boolean add(S3ObjectSummary s) {
				keys.add(s.getKey());
				logger.debugf("%s", s.getKey());
				return true;
			}
		});
	}

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

}
