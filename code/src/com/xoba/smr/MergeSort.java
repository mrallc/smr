package com.xoba.smr;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.xoba.smr.impl.AsciiTSVReader;
import com.xoba.smr.impl.AsciiTSVWriter;
import com.xoba.smr.impl.StringsKVComparator;
import com.xoba.smr.inf.ICollector;
import com.xoba.smr.inf.IKeyValueComparator;
import com.xoba.smr.inf.IKeyValueReader;
import com.xoba.smr.inf.IKeyValueWriter;
import com.xoba.util.ILogger;
import com.xoba.util.LogFactory;

public class MergeSort {

	private static final ILogger logger = LogFactory.getDefault().create();

	public static void main(String[] args) throws Exception {

		AWSCredentials aws = SimpleMapReduce.createCreds();

		if (false) {
			for (int j = 0; j < 10; j++) {
				File in = new File("/tmp/in.txt.gz");
				PrintWriter pw = new PrintWriter(new OutputStreamWriter(createOutput(in)));
				for (int i = 0; i < 10000; i++) {
					pw.printf("%s\t%d", UUID.randomUUID(), 1);
					pw.println();
				}
				pw.close();

				logger.debugf("putting %,d", j);
				new AmazonS3Client(aws).putObject("smr-in", j + ".gz", in);
			}
		}

		File in = new File("/tmp/in.txt.gz");
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(createOutput(in)));
		for (int i = 0; i < 1000000; i++) {
			pw.printf("%s\t%d", UUID.randomUUID(), i);
			pw.println();
		}
		pw.close();

		logger.debugf("%,d bytes in %s", in.length(), in);

		File out = new File("/tmp/out.txt.gz");

		long start = System.currentTimeMillis();
		mergeSort(10000000, in, out, new StringsKVComparator(), new AsciiTSVReader(), new AsciiTSVWriter());
		logger.debugf("sorted in %,d millis", System.currentTimeMillis() - start);
	}

	public static void mergeSort(long maxSortSize, File in, File out, IKeyValueComparator comp, IKeyValueReader reader,
			final IKeyValueWriter writer) throws Exception {
		mergeSort(maxSortSize, in, out, comp, reader, writer, 0);
	}

	private static void mergeSort(long maxSortSize, File in, File out, IKeyValueComparator comp,
			IKeyValueReader reader, final IKeyValueWriter writer, int level) throws Exception {

		if (in.length() < maxSortSize) {
			sort(in, out, comp, reader, writer);
		} else {

			logger.debugf("splitting %,d bytes...", in.length());

			final CountingInputStream in0 = new CountingInputStream(new FileInputStream(in));

			final long half = in.length() / 2;

			File leftSorted = File.createTempFile("leftsorted_" + level + "_", ".dat");
			try {
				File rightSorted = File.createTempFile("rightsorted_" + level + "_", ".dat");
				try {
					File leftUnsorted = File.createTempFile("left_" + level + "_", ".dat");
					try {
						File rightUnsorted = File.createTempFile("right_" + level + "_", ".dat");
						try {
							final OutputStream leftOutput = createOutput(leftUnsorted);
							final OutputStream rightOutput = createOutput(rightUnsorted);
							reader.readFully(new GZIPInputStream(new BufferedInputStream(in0)), new ICollector() {
								@Override
								public void collect(byte[] key, byte[] value) throws Exception {
									if (in0.getCount() < half) {
										writer.write(leftOutput, key, value);
									} else {
										writer.write(rightOutput, key, value);
									}
								}
							});
							leftOutput.close();
							rightOutput.close();

							mergeSort(maxSortSize, rightUnsorted, rightSorted, comp, reader, writer, level + 1);
						} finally {
							rightUnsorted.delete();
						}
						mergeSort(maxSortSize, leftUnsorted, leftSorted, comp, reader, writer, level + 1);
					} finally {
						leftUnsorted.delete();
					}

					merge(leftSorted, rightSorted, out, comp, reader, writer);

				} finally {
					rightSorted.delete();
				}
			} finally {
				leftSorted.delete();
			}
		}
	}

	private static interface QPair {

		public Pair getPair();

		public Exception getException();

	}

	private static void merge(File left, File right, File out, final IKeyValueComparator comp, IKeyValueReader reader,
			IKeyValueWriter writer) throws Exception {

		final BlockingQueue<QPair> lq = new LinkedBlockingQueue<QPair>(10);
		final BlockingQueue<QPair> rq = new LinkedBlockingQueue<QPair>(10);

		driveAsync(left, lq, reader);
		driveAsync(right, rq, reader);

		Comparator<Pair> c = new Comparator<Pair>() {
			@Override
			public int compare(Pair o1, Pair o2) {
				return comp.compare(o1.key, o2.key, o1.value, o2.value);
			}
		};

		OutputStream os = createOutput(out);
		try {
			Pair currentLeft = null;
			Pair currentRight = null;

			boolean leftDone = false;
			boolean rightDone = false;

			boolean done = false;
			while (!done) {
				if (!leftDone && currentLeft == null) {
					QPair qp = lq.take();
					if (qp.getException() != null) {
						throw qp.getException();
					} else if (qp.getPair() == null) {
						leftDone = true;
					} else {
						currentLeft = qp.getPair();
					}
				}

				if (!rightDone && currentRight == null) {
					QPair qp = rq.take();
					if (qp.getException() != null) {
						throw qp.getException();
					} else if (qp.getPair() == null) {
						rightDone = true;
					} else {
						currentRight = qp.getPair();
					}
				}

				boolean wl = false;
				boolean wr = false;

				if (leftDone && rightDone) {
					done = true;
				} else if (currentLeft == null) {
					wr = true;
				} else if (currentRight == null) {
					wl = true;
				} else {
					int cmp = c.compare(currentLeft, currentRight);
					if (cmp < 0) {
						wl = true;
					} else if (cmp > 0) {
						wr = true;
					} else {
						wl = true;
						wr = true;
					}
				}

				if (wl) {
					writer.write(os, currentLeft.key, currentLeft.value);
					currentLeft = null;
				}

				if (wr) {
					writer.write(os, currentRight.key, currentRight.value);
					currentRight = null;
				}
			}
		} finally {
			os.close();
		}

	}

	private static QPair wrap(final Pair p) {
		return new QPair() {

			@Override
			public Pair getPair() {
				return p;
			}

			@Override
			public Exception getException() {
				return null;
			}
		};
	}

	private static QPair wrap(final Exception p) {
		return new QPair() {

			@Override
			public Pair getPair() {
				return null;
			}

			@Override
			public Exception getException() {
				return p;
			}
		};
	}

	private static void driveAsync(final File f, final BlockingQueue<QPair> q, final IKeyValueReader reader)
			throws Exception {
		new Thread() {
			@Override
			public void run() {
				try {
					reader.readFully(createInput(f), new ICollector() {
						@Override
						public void collect(byte[] key, byte[] value) throws Exception {
							q.put(wrap(new Pair(key, value)));
						}
					});

					q.put(new QPair() {

						@Override
						public Pair getPair() {
							return null;
						}

						@Override
						public Exception getException() {
							return null;
						}

					});
				} catch (Exception e) {
					logger.warnf("got exception reading %s: %s", f, e);
					q.add(wrap(e));
				}
			}
		}.start();
	}

	private static class Pair {

		@Override
		public String toString() {
			return new Formatter().format("%s\t%s", new String(key), new String(value)).toString();
		}

		public Pair(byte[] key, byte[] value) {
			this.key = key;
			this.value = value;
		}

		public byte[] key, value;

	}

	private static void sort(File in, File out, final IKeyValueComparator comp, IKeyValueReader reader,
			IKeyValueWriter writer) throws Exception {

		final List<Pair> list = new LinkedList<Pair>();
		InputStream in0 = createInput(in);
		try {
			reader.readFully(in0, new ICollector() {
				@Override
				public void collect(byte[] key, byte[] value) throws Exception {
					list.add(new Pair(key, value));
				}
			});
		} finally {
			in0.close();
		}

		Collections.sort(list, new Comparator<Pair>() {
			@Override
			public int compare(Pair o1, Pair o2) {
				return comp.compare(o1.key, o2.key, o1.value, o2.value);
			}
		});

		OutputStream out0 = createOutput(out);
		try {
			for (Pair p : list) {
				writer.write(out0, p.key, p.value);
			}
		} finally {
			out0.close();
		}
	}

	private static InputStream createInput(File f) throws Exception {
		return new GZIPInputStream(new BufferedInputStream(new FileInputStream(f)));
	}

	private static OutputStream createOutput(File f) throws Exception {
		return new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
	}

}
