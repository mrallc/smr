package com.xoba.smr;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.Collection;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.handlers.AbstractRequestHandler;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.xoba.util.ILogger;
import com.xoba.util.LogFactory;
import com.xoba.util.MraUtils;

public class SimpleMapReduce {

	private static final ILogger logger = LogFactory.getDefault().create();

	public static void launch(Properties config, Collection<String> inputSplitPrefixes, AmazonInstance ai,
			int machineCount) throws Exception {
		launch(config, inputSplitPrefixes, ai, machineCount, true);
	}

	public static void launch(Properties config, Collection<String> inputSplitPrefixes, AmazonInstance ai,
			int machineCount, boolean spotInstances) throws Exception {

		if (!config.containsKey(ConfigKey.JOB_ID.toString())) {
			config.put(ConfigKey.JOB_ID.toString(), MraUtils.md5Hash(new TreeMap<Object, Object>(config).toString())
					.substring(0, 8));
		}

		String id = config.getProperty(ConfigKey.JOB_ID.toString());

		AWSCredentials aws = create(config);

		AmazonSimpleDB db = new AmazonSimpleDBClient(aws);
		AmazonSQS sqs = new AmazonSQSClient(aws);
		AmazonS3 s3 = new AmazonS3Client(aws);

		final String mapQueue = config.getProperty(ConfigKey.MAP_QUEUE.toString());
		final String reduceQueue = config.getProperty(ConfigKey.REDUCE_QUEUE.toString());

		final String dom = config.getProperty(ConfigKey.SIMPLEDB_DOM.toString());

		final String mapInputBucket = config.getProperty(ConfigKey.MAP_INPUTS_BUCKET.toString());
		final String shuffleBucket = config.getProperty(ConfigKey.SHUFFLE_BUCKET.toString());
		final String reduceOutputBucket = config.getProperty(ConfigKey.REDUCE_OUTPUTS_BUCKET.toString());

		final int hashCard = new Integer(config.getProperty(ConfigKey.HASH_CARDINALITY.toString()));

		s3.createBucket(reduceOutputBucket);

		final long inputSplitCount = inputSplitPrefixes.size();

		for (String key : inputSplitPrefixes) {
			Properties p = new Properties();
			p.setProperty("input", "s3://" + mapInputBucket + "/" + key);
			sqs.sendMessage(new SendMessageRequest(mapQueue, serialize(p)));
		}

		SimpleDbCommitter.commitNewAttribute(db, dom, "parameters", "splits", "" + inputSplitCount);
		SimpleDbCommitter.commitNewAttribute(db, dom, "parameters", "hashes", "" + hashCard);
		SimpleDbCommitter.commitNewAttribute(db, dom, "parameters", "done", "1");

		for (String hash : getAllHashes(hashCard)) {
			Properties p = new Properties();
			p.setProperty("input", "s3://" + shuffleBucket + "/" + hash);
			sqs.sendMessage(new SendMessageRequest(reduceQueue, serialize(p)));
		}

		if (machineCount == 1) {

			// run locally
			SMRDriver.main(new String[] { MraUtils.convertToHex(serialize(config).getBytes()) });

		} else if (machineCount > 1) {

			// run in the cloud

			String userData = produceUserData(ai, config,
					new URI(config.getProperty(ConfigKey.RUNNABLE_JARFILE_URI.toString())));

			logger.debugf("launching job with id %s:", id);

			System.out.println(userData);

			AmazonEC2 ec2 = new AmazonEC2Client(aws);

			if (spotInstances) {

				RequestSpotInstancesRequest sir = new RequestSpotInstancesRequest();
				sir.setSpotPrice(new Double(ai.getPrice()).toString());
				sir.setInstanceCount(machineCount);
				sir.setType("one-time");

				LaunchSpecification spec = new LaunchSpecification();
				spec.setImageId(ai.getDefaultAMI());
				spec.setUserData(new String(new Base64().encode(userData.getBytes("US-ASCII"))));
				spec.setInstanceType(ai.getApiName());

				sir.setLaunchSpecification(spec);

				RequestSpotInstancesResult result = ec2.requestSpotInstances(sir);

				logger.debugf("spot instance request result: %s", result);

			} else {

				RunInstancesRequest req = new RunInstancesRequest(ai.getDefaultAMI(), machineCount, machineCount);
				req.setClientToken(id);
				req.setInstanceType(ai.getApiName());
				req.setInstanceInitiatedShutdownBehavior("terminate");
				req.setUserData(new String(new Base64().encode(userData.getBytes("US-ASCII"))));
				RunInstancesResult resp = ec2.runInstances(req);
				logger.debugf("on demand reservation id: %s", resp.getReservation().getReservationId());
				labelEc2Instance(ec2, resp, "smr-" + id);
			}
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

		pw.printf("java -Xmx%.0fm -cp %s %s %s", 1000 * 0.8 * ai.getMemoryGB(), jar, SMRDriver.class.getName(),
				MraUtils.convertToHex(serialize(c).getBytes()));
		pw.println();

		pw.println("poweroff");

		pw.close();

		return sw.toString();

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

	public static String prefixedName(String p, String n) {
		return p + "-" + n;
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
		if (places == 0) {
			places = 1;
		}
		return new Formatter().format("%0" + places + "d", x).toString();
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
