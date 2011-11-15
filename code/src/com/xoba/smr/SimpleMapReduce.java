package com.xoba.smr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.handlers.AbstractRequestHandler;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.xoba.amazonaws.AWSUtils;
import com.xoba.smr.impl.AsciiTSVReader;
import com.xoba.smr.impl.AsciiTSVWriter;
import com.xoba.smr.impl.ValuePrefixCountingMapper;
import com.xoba.smr.impl.ValueSummingReducer;
import com.xoba.util.ILogger;
import com.xoba.util.LogFactory;
import com.xoba.util.MraUtils;

public class SimpleMapReduce {

	private static final ILogger logger = LogFactory.getDefault().create();

	public static void main(String[] args) throws Exception {

		final long testSequence = System.currentTimeMillis() % 10000;

		logger.debugf("test sequence = %d", testSequence);

		final int hashCardinality = 3;
		final int machineCount = 1;

		AWSCredentials aws = createCreds();

		final String uniquePrefix = "smr-" + MraUtils.md5Hash(aws.getAWSAccessKeyId()).substring(0, 8) + "-"
				+ testSequence;

		final String inputBucket = "...";

		AmazonSimpleDB db = new AmazonSimpleDBClient(aws);
		AmazonSQS sqs = new AmazonSQSClient(aws);

		Properties config = createIdempotentContext(aws, uniquePrefix, inputBucket);

		final String mapQueue = config.getProperty(ConfigKey.MAP_QUEUE.toString());
		final String reduceQueue = config.getProperty(ConfigKey.REDUCE_QUEUE.toString());

		final String dom = config.getProperty(ConfigKey.SIMPLEDB_DOM.toString());

		final String mapInputBucket = config.getProperty(ConfigKey.MAP_INPUTS_BUCKET.toString());
		final String shuffleBucket = config.getProperty(ConfigKey.SHUFFLE_BUCKET.toString());
		final String reduceOutputBucket = config.getProperty(ConfigKey.REDUCE_OUTPUTS_BUCKET.toString());

		List<String> inputSplitPrefixes = new LinkedList<String>();

		{
			// add key prefixes to inputSplitPrefixes here...
		}

		logger.debugf("using %,d key prefixes", inputSplitPrefixes.size());

		final long inputSplitCount = inputSplitPrefixes.size();

		for (String key : inputSplitPrefixes) {
			Properties p = new Properties();
			p.setProperty("input", "s3://" + mapInputBucket + "/" + key);
			p.setProperty("output", "s3://" + shuffleBucket);
			p.setProperty("hashes", "" + hashCardinality);
			sqs.sendMessage(new SendMessageRequest(mapQueue, serialize(p)));
		}

		SimpleDbCommitter.commitNewAttribute(db, dom, "parameters", "splits", "" + inputSplitCount);
		SimpleDbCommitter.commitNewAttribute(db, dom, "parameters", "hashes", "" + hashCardinality);
		SimpleDbCommitter.commitNewAttribute(db, dom, "parameters", "done", "1");

		for (String hash : getAllHashes(hashCardinality)) {
			Properties p = new Properties();
			p.setProperty("input", "s3://" + shuffleBucket + "/" + hash);
			p.setProperty("output", "s3://" + reduceOutputBucket);
			sqs.sendMessage(new SendMessageRequest(reduceQueue, serialize(p)));
		}

		if (machineCount == 1) {
			// run locally
			SMRDriver.main(new String[] { MraUtils.convertToHex(serialize(config).getBytes()) });
		} else if (machineCount > 1) {
			// run in the cloud
			AmazonEC2 ec2 = new AmazonEC2Client(aws);
			AmazonInstance ai = AmazonInstance.M2_4XLARGE;
			String ud = produceUserData(ai, config,
					new URI(config.getProperty(ConfigKey.RUNNABLE_JARFILE_URI.toString())));
			System.out.println(ud);
			RunInstancesRequest req = new RunInstancesRequest(ai.getDefaultAMI(), machineCount, machineCount);
			req.setInstanceType(ai.getApiName());
			req.setKeyName("mrascratch");
			req.setInstanceInitiatedShutdownBehavior("terminate");
			req.setUserData(new String(new Base64().encode(ud.getBytes("US-ASCII"))));
			RunInstancesResult resp = ec2.runInstances(req);
			logger.debugf("reservation id = %s", resp.getReservation().getReservationId());
			labelEc2Instance(ec2, resp, "test");
		}
	}

	public static void labelEc2Instance(AmazonEC2 ec2, RunInstancesResult resp, String title) throws Exception {
		int tries = 0;
		boolean done = false;
		while (tries++ < 3 && !done) {
			try {
				List<String> resources = new LinkedList<String>();
				for (Instance i : resp.getReservation().getInstances()) {
					resources.add(i.getInstanceId());
				}
				List<Tag> tags = new LinkedList<Tag>();
				tags.add(new Tag("Name", title));
				CreateTagsRequest ctr = new CreateTagsRequest(resources, tags);
				ec2.createTags(ctr);
				done = true;
				logger.debugf("set tag(s)");
			} catch (Exception e) {
				logger.warnf("exception setting tags: %s", e);
				Thread.sleep(3000);
			}
		}

	}

	public static String produceUserData(AmazonInstance ai, Properties c, URI jarFileURI) throws Exception {

		StringWriter sw = new StringWriter();
		LinuxLineConventionPrintWriter pw = new LinuxLineConventionPrintWriter(new PrintWriter(sw));
		pw.println("#!/bin/sh");
		pw.println("cd /root");

		pw.println("chmod 777 /mnt");
		pw.println("aptitude update");
		Set<String> set = new TreeSet<String>();

		set.add("openjdk-6-jdk");
		set.add("wget");

		if (set.size() > 0) {
			pw.print("aptitude install -y ");
			Iterator<String> it = set.iterator();
			while (it.hasNext()) {
				String x = it.next();
				pw.print(x);
				if (it.hasNext()) {
					pw.print(" ");
				}
			}
			pw.println();
		}

		pw.printf("wget %s", jarFileURI);
		pw.println();

		String[] parts = jarFileURI.getPath().split("/");
		String jar = parts[parts.length - 1];

		pw.printf("java -Xmx%.0fm -jar %s %s", 1000 * 0.8 * ai.getMemoryGB(), jar,
				MraUtils.convertToHex(serialize(c).getBytes()));
		pw.println();

		pw.println("poweroff");

		pw.close();

		return sw.toString();

	}

	private static void populateTestBucket(int n, String bucket) throws Exception {

		AWSCredentials aws = createCreds();

		AmazonS3 s3 = getS3(aws);

		s3.createBucket(bucket);

		for (int i = 0; i < n; i++) {
			logger.debugf("input %,d", i);
			File tmp = File.createTempFile("data", ".gz");
			try {

				PrintWriter pw = new PrintWriter(new OutputStreamWriter(SMRDriver.createCompressingOutput(tmp)));
				try {
					for (int j = 0; j < 10000; j++) {
						pw.printf("%d\t%s", j, UUID.randomUUID());
						pw.println();
					}
				} finally {
					pw.close();
				}

				String key = "split-" + i + ".gz";

				s3.putObject(bucket, key, tmp);

			} finally {
				tmp.delete();
			}
		}

	}

	public static AmazonS3 getS3(AWSCredentials aws) {
		AmazonS3Client s3 = new AmazonS3Client(aws);
		s3.addRequestHandler(new AbstractRequestHandler() {
			@Override
			public void beforeRequest(Request<?> request) {
				request.addHeader("x-amz-request-payer", "requester");
			}
		});
		return s3;
	}

	/**
	 * creates configuration which drives the whole mapreduce process
	 */
	public static Properties createIdempotentContext(AWSCredentials aws, String prefix, String inputBucket)
			throws Exception {

		AmazonS3 s3 = getS3(aws);
		AmazonSimpleDB db = new AmazonSimpleDBClient(aws);
		AmazonSQS sqs = new AmazonSQSClient(aws);

		Map<ConfigKey, Object> out = new HashMap<ConfigKey, Object>();

		out.put(ConfigKey.AWS_KEYID, aws.getAWSAccessKeyId());
		out.put(ConfigKey.AWS_SECRETKEY, aws.getAWSSecretKey());

		out.put(ConfigKey.MAPPER, ValuePrefixCountingMapper.class);
		out.put(ConfigKey.REDUCER, ValueSummingReducer.class);

		out.put(ConfigKey.MAP_QUEUE, sqs.createQueue(new CreateQueueRequest(prefixedName(prefix, "map"))).getQueueUrl());
		out.put(ConfigKey.REDUCE_QUEUE, sqs.createQueue(new CreateQueueRequest(prefixedName(prefix, "reduce")))
				.getQueueUrl());

		out.put(ConfigKey.SIMPLEDB_DOM, prefixedName(prefix, "commits").replaceAll("-", ""));
		out.put(ConfigKey.SHUFFLE_BUCKET, prefixedName(prefix, "shuffle"));
		out.put(ConfigKey.REDUCE_OUTPUTS_BUCKET, prefixedName(prefix, "reduceoutputs"));

		out.put(ConfigKey.MAP_READER, "com.xoba.smr.JSonKVReader");
		out.put(ConfigKey.IS_INPUT_COMPRESSED, true);
		out.put(ConfigKey.MAP_INPUTS_BUCKET, inputBucket);

		out.put(ConfigKey.SHUFFLEWRITER, AsciiTSVWriter.class);
		out.put(ConfigKey.SHUFFLEREADER, AsciiTSVReader.class);
		out.put(ConfigKey.REDUCEWRITER, AsciiTSVWriter.class);

		out.put(ConfigKey.RUNNABLE_JARFILE_URI, new URI("http://bogus.com"));
		out.put(ConfigKey.CLASSPATH_JAR, new URI("file:///tmp/bogus.jar"));

		Properties properties = new Properties();

		for (ConfigKey c : out.keySet()) {
			Object o = out.get(c);
			if (o instanceof String || o instanceof URI || o instanceof Boolean) {
				properties.put(c.toString(), o.toString());
			} else if (o instanceof Class) {
				Class<?> x = (Class<?>) o;
				properties.put(c.toString(), x.getName());
			} else {
				throw new IllegalStateException();
			}
		}

		db.createDomain(new CreateDomainRequest(properties.getProperty(ConfigKey.SIMPLEDB_DOM.toString())));

		try {
			s3.createBucket(properties.getProperty(ConfigKey.MAP_INPUTS_BUCKET.toString()));
		} catch (Exception e) {
			logger.warnf("can't create map input bucket: %s", e);
		}

		s3.createBucket(properties.getProperty(ConfigKey.SHUFFLE_BUCKET.toString()));
		s3.createBucket(properties.getProperty(ConfigKey.REDUCE_OUTPUTS_BUCKET.toString()));

		Set<String> keys = new TreeSet<String>();
		for (Object o : properties.keySet()) {
			keys.add(o.toString());
		}

		for (String o : keys) {
			logger.debugf("config %s = %s", o, properties.get(o));
		}

		return properties;
	}

	@SuppressWarnings("unchecked")
	public static <T> T load(ClassLoader cl, Properties p, ConfigKey c, Class<T> x) throws Exception {
		T y = (T) cl.loadClass(p.getProperty(c.toString())).newInstance();
		logger.debugf("%s -> %s", c, y);
		return y;
	}

	public static String prefixedName(String p, String n) {
		return p + "-" + n;
	}

	@SuppressWarnings("unused")
	private static void cleanup(Properties p) throws Exception {
		AWSCredentials aws = create(p);
		AmazonS3 s3 = getS3(aws);
		AmazonSimpleDB db = new AmazonSimpleDBClient(aws);
		AmazonSQS sqs = new AmazonSQSClient(aws);

		db.deleteDomain(new DeleteDomainRequest(p.getProperty(ConfigKey.SIMPLEDB_DOM.toString())));

		sqs.deleteQueue(new DeleteQueueRequest(p.getProperty(ConfigKey.MAP_QUEUE.toString())));
		sqs.deleteQueue(new DeleteQueueRequest(p.getProperty(ConfigKey.REDUCE_QUEUE.toString())));

		AWSUtils.deleteBucket(s3, p.getProperty(ConfigKey.MAP_INPUTS_BUCKET.toString()));

		AWSUtils.deleteBucket(s3, p.getProperty(ConfigKey.SHUFFLE_BUCKET.toString()));
		AWSUtils.deleteBucket(s3, p.getProperty(ConfigKey.REDUCE_OUTPUTS_BUCKET.toString()));
	}

	public static AWSCredentials create(final Properties p) {
		return new AWSCredentials() {

			@Override
			public String getAWSSecretKey() {
				return p.getProperty(ConfigKey.AWS_SECRETKEY.toString());
			}

			@Override
			public String getAWSAccessKeyId() {
				return p.getProperty(ConfigKey.AWS_KEYID.toString());
			}
		};
	}

	public static SortedSet<String> getAllHashes(long mod) {
		SortedSet<String> out = new TreeSet<String>();
		for (long i = 0; i < mod; i++) {
			out.add(fmt(i, mod));
		}
		return out;
	}

	public static String hash(byte[] key, long mod) {
		byte[] buf = MraUtils.md5HashBytesToBytes(key);
		long x = Math.abs(MraUtils.extractLongValue(buf));
		return fmt(x % mod, mod);
	}

	private static String fmt(long x, long mod) {
		long places = Math.round(Math.ceil(Math.log10(mod)));
		return new Formatter().format("%0" + places + "d", x).toString();
	}

	private static AWSCredentials CACHED_AWS_CREDENTIALS = null;

	public static synchronized AWSCredentials createCreds() throws IOException {

		if (CACHED_AWS_CREDENTIALS == null) {
			File aws = new File(System.getProperty("user.home") + "/.awssecret");

			BufferedReader reader = new BufferedReader(new FileReader(aws));

			final String accessKeyID = reader.readLine();
			final String secretKey = reader.readLine();

			logger.debugf("using aws credentials %s and %s", accessKeyID, secretKey);

			CACHED_AWS_CREDENTIALS = new AWSCredentials() {

				@Override
				public String getAWSAccessKeyId() {
					return accessKeyID;
				}

				@Override
				public String getAWSSecretKey() {
					return secretKey;
				}
			};
		}

		return CACHED_AWS_CREDENTIALS;
	}

	public static String serialize(Properties p) throws Exception {
		StringWriter sw = new StringWriter();
		try {
			p.store(sw, "n/a");
		} finally {
			sw.close();
		}
		return sw.toString();
	}

	public static Properties marshall(String s) throws Exception {
		Properties p = new Properties();
		StringReader sr = new StringReader(s);
		try {
			p.load(sr);
		} finally {
			sr.close();
		}
		return p;
	}

}
