package com.xoba.smr;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.xoba.smr.be.IBackend;
import com.xoba.smr.be.IInputFile;
import com.xoba.smr.be.MapperAWSBackend;
import com.xoba.smr.be.ReducerAWSBackend;
import com.xoba.smr.be.WorkUnit;
import com.xoba.smr.inf.ICollector;
import com.xoba.smr.inf.IKeyValueComparator;
import com.xoba.smr.inf.IKeyValueReader;
import com.xoba.smr.inf.IKeyValueWriter;
import com.xoba.smr.inf.IMapper;
import com.xoba.smr.inf.IMappingContext;
import com.xoba.smr.inf.IReducer;
import com.xoba.util.ILogger;
import com.xoba.util.LogFactory;
import com.xoba.util.MraUtils;

public class SMRDriver {

	private static final ILogger logger = LogFactory.getDefault().create();

	private static final boolean DEBUG_MODE = false;

	public static void main(String[] args) throws Exception {

		final Properties c = SimpleMapReduce.marshall(new String(MraUtils.convertFromHex(args[0])));

		for (Object o : c.keySet()) {
			logger.debugf("%s = %s", o, c.get(o));
		}

		URI jar = c.containsKey(ConfigKey.CLASSPATH_JAR.toString()) ? new URI(c.getProperty(ConfigKey.CLASSPATH_JAR
				.toString())) : null;

		ClassLoader cl = (jar == null ? Thread.currentThread().getContextClassLoader() : new URLClassLoader(
				new URL[] { jar.toURL() }));

		final IMapper mapper = loadClass(cl, c, ConfigKey.MAPPER, IMapper.class);
		final IReducer reducer = loadClass(cl, c, ConfigKey.REDUCER, IReducer.class);

		final IKeyValueReader mapReader = loadClass(cl, c, ConfigKey.MAP_READER, IKeyValueReader.class);
		final IKeyValueWriter mapWriter = loadClass(cl, c, ConfigKey.SHUFFLEWRITER, IKeyValueWriter.class);
		final IKeyValueReader reduceReader = SMRDriver.loadClass(cl, c, ConfigKey.SHUFFLEREADER, IKeyValueReader.class);
		final IKeyValueWriter reduceWriter = loadClass(cl, c, ConfigKey.REDUCEWRITER, IKeyValueWriter.class);
		final IKeyValueComparator comp = loadClass(cl, c, ConfigKey.SHUFFLE_COMPARATOR, IKeyValueComparator.class);

		final long hashes = new Long(c.getProperty(ConfigKey.HASH_CARDINALITY.toString()));

		int threadCount = c.containsKey(ConfigKey.THREADS_PER_MACHINE.toString()) ? new Integer(
				c.getProperty(ConfigKey.THREADS_PER_MACHINE.toString())) : Runtime.getRuntime().availableProcessors();

		List<Thread> threads = new LinkedList<Thread>();

		for (int i = 0; i < threadCount; i++) {

			Thread t = new Thread() {
				@Override
				public void run() {

					boolean done = false;

					while (!done) {
						try {

							runMapper(new MapperAWSBackend(c), mapReader, mapWriter, mapper,
									new Boolean(c.getProperty(ConfigKey.IS_INPUT_COMPRESSED.toString())), hashes);

							runReducer(new ReducerAWSBackend(c), reduceReader, reduceWriter, reducer, comp);

							done = true;

						} catch (Exception e) {

							logger.warnf("exception in main loop: %s", e);

							try {
								Thread.sleep(1000);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}

						} finally {
							logger.debugf("thread exiting");
						}
					}

				}
			};

			t.start();

			threads.add(t);
		}

		boolean done = false;
		boolean interrupted = false;

		while (!done) {
			for (Thread t : threads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					interrupted = true;
				}
			}
			if (!interrupted) {
				done = true;
			}
		}

	}

	public static String extractBucket(URI u) throws Exception {
		if (u.getScheme().equals("s3")) {
			if (u.isOpaque()) {
				String ssp = u.getSchemeSpecificPart();
				int index = ssp.indexOf('/');
				if (index == -1) {
					return ssp;
				} else {
					return ssp.substring(0, index);
				}
			} else {
				return u.getHost();
			}
		}
		throw new IllegalArgumentException(u.toString());
	}

	public static String extractKey(URI u) throws Exception {
		if (u.getScheme().equals("s3")) {
			if (u.isOpaque()) {
				String ssp = u.getSchemeSpecificPart();
				int index = ssp.indexOf('/');
				if (index == -1) {
					throw new IllegalArgumentException("bad uri: " + u);
				} else {
					return ssp.substring(index + 1, ssp.length());
				}
			} else {
				return u.getPath().substring(1);
			}
		}
		throw new IllegalArgumentException(u.toString());
	}

	public static void runMapper(IBackend<WorkUnit> be, final IKeyValueReader reader, final IKeyValueWriter writer,
			final IMapper mapper, final boolean isInputCompressed, final long hashes) throws Exception {

		try {
			be.sendNotification("mapping");
		} catch (Exception e) {
			logger.warnf("can't send notification: %s", e);
		}

		boolean done = false;
		while (!done) {

			try {

				WorkUnit p = be.getNextWorkUnit();

				if (p == null) {
					Thread.sleep(1000);
				} else {
					try {
						logger.debugf("processing %s", p);

						URI in = new URI(p.getProperty("input"));

						final Map<String, File> files = new HashMap<String, File>();
						final Map<String, OutputStream> writers = new HashMap<String, OutputStream>();

						for (String hash : SimpleMapReduce.getAllHashes(hashes)) {
							File tmp = createTempFile("mapper hash output " + hash, "output", ".gz");
							OutputStream pw = new BufferedOutputStream(createCompressingOutput(tmp));
							files.put(hash, tmp);
							writers.put(hash, pw);
						}

						List<IInputFile> inputFiles = be.getInputFilesForWorkUnit(p);

						for (final IInputFile f : inputFiles) {

							File split = createTempFile("mapper input split " + f.getURI(), "split", ".raw");
							try {
								be.getInputFile(f.getURI(), split);

								mapper.beginContext(new IMappingContext() {

									@Override
									public IInputFile getInputSplit() {
										return f;
									}

								});

								final ICollector collector = new ICollector() {
									@Override
									public void collect(byte[] key, byte[] value) throws Exception {
										writer.write(writers.get(SimpleMapReduce.hash(key, hashes)), key, value);
									}
								};

								reader.readFully(isInputCompressed ? createDecompressingInput(split)
										: createInput(split), new ICollector() {
									@Override
									public void collect(byte[] key, byte[] value) throws Exception {
										mapper.map(key, value, collector);
									}
								});

							} finally {
								delete(split);
							}

						}

						for (OutputStream pw : writers.values()) {
							pw.close();
						}

						for (String h : files.keySet()) {
							File f = files.get(h);
							String key = h + "/" + extractKey(in);
							be.putResultFile(key, f);
							delete(f);
						}

						be.commitWork(p);

					} finally {
						be.releaseWork(p);
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
				Thread.sleep(1000);
			} finally {
				try {
					done = be.isDone();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

	private static InputStream createInput(File f) throws Exception {
		return new BufferedInputStream(new FileInputStream(f));
	}

	private static InputStream createDecompressingInput(File f) throws Exception {
		return new GZIPInputStream(new BufferedInputStream(new FileInputStream(f)));
	}

	public static OutputStream createCompressingOutput(File f) throws Exception {
		return new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
	}

	private static class Envelope<T> {
		public T item;
	}

	public static void runReducer(IBackend<WorkUnit> be, final IKeyValueReader reader, final IKeyValueWriter writer,
			final IReducer reducer, IKeyValueComparator comp) throws Exception {

		try {
			be.sendNotification("reducing");
		} catch (Exception e) {
			logger.warnf("can't send notification: %s", e);
		}

		boolean done = false;
		while (!done) {

			try {

				WorkUnit p = be.getNextWorkUnit();

				if (p == null) {
					Thread.sleep(1000);
				} else {

					logger.debugf("processing %s", p);

					try {

						List<IInputFile> inputFiles = be.getInputFilesForWorkUnit(p);

						File all = createTempFile("sum of shuffles", "all", ".gz");
						final OutputStream pw = createCompressingOutput(all);
						try {
							for (IInputFile k : inputFiles) {

								File tmp = createTempFile("shuffle " + k, "file", ".gz");
								try {
									be.getInputFile(k.getURI(), tmp);
									reader.readFully(createDecompressingInput(tmp), new ICollector() {
										@Override
										public void collect(byte[] key9, byte[] value9) throws Exception {
											writer.write(pw, key9, value9);
										}
									});

								} finally {
									delete(tmp);
								}
							}
						} finally {
							pw.close();
						}

						File sorted = createTempFile("sorted shuffle", "sorted", ".gz");
						MergeSort.mergeSort(100000000, all, sorted, comp, reader, writer);

						File out2 = createTempFile("reducer output", "output", ".gz");
						try {

							final OutputStream reducerOutput = createCompressingOutput(out2);
							try {

								final ICollector collector = new ICollector() {
									@Override
									public void collect(byte[] key, byte[] value) throws Exception {
										writer.write(reducerOutput, key, value);
									}
								};

								final List<byte[]> currentValues = new LinkedList<byte[]>();
								final Envelope<MyByteBuffer> currentKey = new Envelope<MyByteBuffer>();

								reader.readFully(createDecompressingInput(sorted), new ICollector() {

									@Override
									public void collect(byte[] key, byte[] value) throws Exception {

										MyByteBuffer b = new MyByteBuffer(key);

										if (currentKey.item != null) {
											if (!currentKey.item.equals(b)) {
												// new key
												reducer.reduce(currentKey.item.buf, currentValues.iterator(), collector);
												currentValues.clear();
											}
										}

										currentValues.add(value);
										currentKey.item = b;
									}
								});

								if (currentValues.size() > 0) {
									reducer.reduce(currentKey.item.buf, currentValues.iterator(), collector);
								}

							} finally {
								reducerOutput.close();
							}

							be.putResultFile(extractKey(new URI(p.getProperty("input"))) + ".gz", out2);

							be.commitWork(p);

						} finally {
							delete(out2);
						}
					} finally {
						be.releaseWork(p);
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
				Thread.sleep(1000);
			} finally {
				try {
					done = be.isDone();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		try {
			be.sendNotification("done");
		} catch (Exception e) {
			logger.warnf("can't send notification: %s", e);
		}

	}

	private static File createTempFile(String msg, String a, String b) throws IOException {
		File f = File.createTempFile(a, b);
		if (DEBUG_MODE) {
			logger.debugf("creating %s as %s", msg, f);
		}
		return f;
	}

	private static void delete(File f) {
		if (!DEBUG_MODE) {
			f.delete();
		}
	}

	private static class MyByteBuffer {

		@Override
		public int hashCode() {
			return hc;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MyByteBuffer other = (MyByteBuffer) obj;
			if (!Arrays.equals(buf, other.buf))
				return false;
			return true;
		}

		public MyByteBuffer(byte[] buf) {
			this.buf = buf;
			this.hc = Arrays.hashCode(buf);
		}

		private final int hc;
		private final byte[] buf;

	}

	@SuppressWarnings("unchecked")
	public static <T> T loadClass(ClassLoader cl, Properties p, ConfigKey c, Class<T> x) throws Exception {
		T y = (T) cl.loadClass(p.getProperty(c.toString())).newInstance();
		logger.debugf("%s -> %s", c, y);
		return y;
	}
}
