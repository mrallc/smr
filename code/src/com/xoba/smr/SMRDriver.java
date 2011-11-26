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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.xoba.amazonaws.AWSUtils;
import com.xoba.amazonaws.AWSUtils.IBucketListener;
import com.xoba.smr.be.IBackend;
import com.xoba.smr.be.IInputFile;
import com.xoba.smr.be.MapperAWSBackend;
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

		final AWSCredentials aws = SimpleMapReduce.create(c);

		URI jar = c.containsKey(ConfigKey.CLASSPATH_JAR.toString()) ? new URI(c.getProperty(ConfigKey.CLASSPATH_JAR
				.toString())) : null;

		ClassLoader cl = (jar == null ? Thread.currentThread().getContextClassLoader() : new URLClassLoader(
				new URL[] { jar.toURL() }));

		final String dom = c.getProperty(ConfigKey.SIMPLEDB_DOM.toString());
		final String mapQueue = c.getProperty(ConfigKey.MAP_QUEUE.toString());
		final String reduceQueue = c.getProperty(ConfigKey.REDUCE_QUEUE.toString());

		final IKeyValueReader mapReader = SimpleMapReduce.load(cl, c, ConfigKey.MAP_READER, IKeyValueReader.class);
		final IKeyValueWriter mapWriter = SimpleMapReduce.load(cl, c, ConfigKey.SHUFFLEWRITER, IKeyValueWriter.class);
		final IKeyValueReader reduceReader = SimpleMapReduce
				.load(cl, c, ConfigKey.SHUFFLEREADER, IKeyValueReader.class);
		final IKeyValueWriter reduceWriter = SimpleMapReduce.load(cl, c, ConfigKey.REDUCEWRITER, IKeyValueWriter.class);

		final IKeyValueComparator comp = SimpleMapReduce.load(cl, c, ConfigKey.SHUFFLE_COMPARATOR,
				IKeyValueComparator.class);

		final IMapper mapper = SimpleMapReduce.load(cl, c, ConfigKey.MAPPER, IMapper.class);
		final IReducer reducer = SimpleMapReduce.load(cl, c, ConfigKey.REDUCER, IReducer.class);

		final String mapOut = c.getProperty(ConfigKey.SHUFFLE_BUCKET.toString());
		final String reduceOut = c.getProperty(ConfigKey.REDUCE_OUTPUTS_BUCKET.toString());
		final long hashes = new Long(c.getProperty(ConfigKey.HASH_CARDINALITY.toString()));

		int threadCount = c.containsKey(ConfigKey.THREADS_PER_MACHINE.toString()) ? new Integer(
				c.getProperty(ConfigKey.THREADS_PER_MACHINE.toString())) : Runtime.getRuntime().availableProcessors();

		logger.debugf("threadcount = %,d", threadCount);

		List<Thread> threads = new LinkedList<Thread>();

		for (int i = 0; i < threadCount; i++) {

			Thread t = new Thread() {
				@Override
				public void run() {

					boolean done = false;

					while (!done) {
						try {

							runMapper(new MapperAWSBackend(c), mapReader, mapWriter, mapper, new Boolean(c.getProperty(ConfigKey.IS_INPUT_COMPRESSED.toString())), hashes);

							logger.debugf("done mapping");

							runReducers(reduceReader, reduceWriter, aws, dom, reduceQueue, reducer, reduceOut, comp);

							logger.debugf("done reducing");

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

					try {

						Thread.sleep(new Random().nextInt(3000));

						if (countCommitted(aws, dom, "sns") < 1) {
							AmazonSimpleDB db = new AmazonSimpleDBClient(aws);
							if (c.containsKey(ConfigKey.SNS_ARN.toString())) {
								String arn = c.getProperty(ConfigKey.SNS_ARN.toString());
								AmazonSNS sns = new AmazonSNSClient(aws);
								sns.publish(new PublishRequest(arn, "done"));
							}
							SimpleDbCommitter.commitNewAttribute(db, dom, "notifications", "sns", "1");
						}

					} catch (Exception e) {
						logger.warnf("failed to send notification: %s", e);
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

	public static void runReducers(final IKeyValueReader reader, final IKeyValueWriter writer, AWSCredentials aws,
			final String dom, final String reduceQueue, final IReducer reducer, String out, IKeyValueComparator comp)
			throws Exception {

		final AmazonS3 s3 = SimpleMapReduce.getS3(aws);
		final AmazonSimpleDB db = new AmazonSimpleDBClient(aws);
		final AmazonSQS sqs = new AmazonSQSClient(aws);

		ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
		try {
			boolean done = false;
			while (!done) {

				try {
					final int timeout = new Integer(sqs
							.getQueueAttributes(
									new GetQueueAttributesRequest(reduceQueue).withAttributeNames("VisibilityTimeout"))
							.getAttributes().get("VisibilityTimeout"));

					ReceiveMessageResult r = sqs.receiveMessage(new ReceiveMessageRequest(reduceQueue));

					for (final Message m : r.getMessages()) {

						ScheduledFuture<?> keepAlive = ses.scheduleAtFixedRate(new Runnable() {
							@Override
							public void run() {
								sqs.changeMessageVisibility(new ChangeMessageVisibilityRequest(reduceQueue, m
										.getReceiptHandle(), timeout));
							}
						}, timeout / 2, timeout, TimeUnit.SECONDS);
						try {
							Properties p = SimpleMapReduce.marshall(m.getBody());

							URI in = new URI(p.getProperty("input"));

							logger.debugf("processing %s to %s", in, out);

							final Set<String> s3Keys = new HashSet<String>();

							AWSUtils.scanObjectsInBucket(s3, extractBucket(in), extractKey(in), new IBucketListener() {

								@Override
								public boolean add(S3ObjectSummary s) {
									s3Keys.add(s.getKey());
									return true;
								}

								@Override
								public void done() {

								}
							});

							if (s3Keys.size() != countCommitted(aws, dom, "mapped")) {
								throw new Exception("missing some map outputs");
							}

							File all = createTempFile("sum of shuffles", "all", ".gz");
							final OutputStream pw = createCompressingOutput(all);
							try {
								for (String k : s3Keys) {

									File tmp = createTempFile("shuffle " + k, "file", ".gz");
									try {
										s3.getObject(new GetObjectRequest(extractBucket(in), k), tmp);
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
									final Envelope<MyByteBuffer> currentKey = new Envelope<SMRDriver.MyByteBuffer>();

									reader.readFully(createDecompressingInput(sorted), new ICollector() {

										@Override
										public void collect(byte[] key, byte[] value) throws Exception {

											MyByteBuffer b = new MyByteBuffer(key);

											if (currentKey.item != null) {
												if (!currentKey.item.equals(b)) {
													// new key
													reducer.reduce(currentKey.item.buf, currentValues.iterator(),
															collector);
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

								s3.putObject(out, extractKey(in) + ".gz", out2);

								SimpleDbCommitter.commitNewAttribute(db, dom, in.toString(), "reduced", "1");

								sqs.deleteMessage(new DeleteMessageRequest(reduceQueue, m.getReceiptHandle()));

							} finally {
								delete(out2);
							}

						} finally {
							keepAlive.cancel(false);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					Thread.sleep(1000);
				} finally {
					try {
						done = isDone(aws, dom, "parameters", "hashes", "reduced");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		} finally {
			ses.shutdown();
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

	private static long countCommitted(AWSCredentials aws, final String dom, String countAttr) throws Exception {

		final AmazonSimpleDB db = new AmazonSimpleDBClient(aws);

		long foundCommitted = 0;
		String expr = "select * from " + dom;
		SelectResult sr = db.select(new SelectRequest(expr, true));
		boolean done = false;
		while (!done) {
			List<Item> items = sr.getItems();
			for (Item i : items) {
				Map<String, String> map = new HashMap<String, String>();
				for (Attribute a : i.getAttributes()) {
					map.put(a.getName(), a.getValue());
				}
				if (map.containsKey(countAttr)) {
					foundCommitted += new Long(map.get(countAttr));
				}
			}
			String t = sr.getNextToken();
			if (t == null) {
				done = true;
			} else {
				SelectRequest req = new SelectRequest(expr, true);
				req.setNextToken(t);
				sr = db.select(req);
			}
		}

		return foundCommitted;
	}

	public static boolean isDone(AWSCredentials aws, final String dom, String item, String targetAttr, String countAttr)
			throws Exception {

		final AmazonSimpleDB db = new AmazonSimpleDBClient(aws);

		Long splits = null;

		long foundCommitted = 0;
		String expr = "select * from " + dom;
		SelectResult sr = db.select(new SelectRequest(expr, true));
		boolean done = false;
		while (!done) {
			List<Item> items = sr.getItems();
			for (Item i : items) {
				String name = i.getName();
				Map<String, String> map = new HashMap<String, String>();
				for (Attribute a : i.getAttributes()) {
					map.put(a.getName(), a.getValue());
				}
				if (name.equals(item)) {
					splits = new Long(map.get(targetAttr));
				} else if (map.containsKey(countAttr)) {
					foundCommitted += new Long(map.get(countAttr));
				}
			}
			String t = sr.getNextToken();
			if (t == null) {
				done = true;
			} else {
				SelectRequest req = new SelectRequest(expr, true);
				req.setNextToken(t);
				sr = db.select(req);
			}
		}

		if (splits == null) {
			return false;
		}

		return splits == foundCommitted;
	}

}
