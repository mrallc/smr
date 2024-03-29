package com.xoba.smr.be;

import java.io.File;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
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
import com.xoba.smr.ConfigKey;
import com.xoba.smr.SMRDriver;
import com.xoba.smr.SimpleDbCommitter;
import com.xoba.smr.SimpleMapReduce;
import com.xoba.util.ILogger;
import com.xoba.util.LogFactory;
import com.xoba.util.MraUtils;

public abstract class AbstractBackend implements IBackend<WorkUnit> {

	private static final ILogger logger = LogFactory.getDefault().create();

	@Override
	public synchronized void sendNotification(final String type) throws Exception {
		logger.debugf("snsNotify: %s", type);
		if (snsARN != null) {
			PublishResult r = SimpleDbCommitter.tryToRunIdempotentTaskOnce(db, dom, "notifications",
					"I" + MraUtils.md5Hash(type), new Callable<PublishResult>() {
						@Override
						public PublishResult call() throws Exception {
							return sns.publish(new PublishRequest(snsARN, jobID + ": " + type));
						}
					});
			if (r != null) {
				logger.debugf("published: %s", r);
			}
		}

	}

	protected final AWSCredentials aws;
	protected final AmazonS3 s3;
	protected final AmazonSimpleDB db;
	protected final AmazonSQS sqs;
	protected final AmazonSNS sns;
	protected final String workQueue, dom, outputBucket;
	protected final ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
	protected final int timeout;
	private final String jobID, snsARN;

	public AbstractBackend(Properties c, ConfigKey queue, ConfigKey outputBucket) {

		this.aws = SimpleMapReduce.create(c);

		this.db = new AmazonSimpleDBClient(aws);
		this.s3 = SimpleMapReduce.getS3(aws);
		this.sqs = new AmazonSQSClient(aws);
		this.sns = new AmazonSNSClient(aws);

		this.dom = c.getProperty(ConfigKey.SIMPLEDB_DOM.toString());

		this.workQueue = c.getProperty(queue.toString());
		this.outputBucket = c.getProperty(outputBucket.toString());

		this.jobID = c.getProperty(ConfigKey.JOB_ID.toString());
		this.snsARN = c.getProperty(ConfigKey.SNS_ARN.toString());

		timeout = new Integer(sqs
				.getQueueAttributes(new GetQueueAttributesRequest(workQueue).withAttributeNames("VisibilityTimeout"))
				.getAttributes().get("VisibilityTimeout"));

	}

	@Override
	public void getInputFile(URI u, File f) throws Exception {
		final String bucket = SMRDriver.extractBucket(u);
		final String key = SMRDriver.extractKey(u);
		s3.getObject(new GetObjectRequest(bucket, key), f);
	}

	protected boolean isDone(String targetAttr, String countAttr) throws Exception {
		boolean done = SimpleDbCommitter.isDone(db, dom, "parameters", targetAttr, countAttr);
		if (done) {
			ses.shutdown();
		}
		return done;
	}

	protected void commitWork(WorkUnit p, String attr) throws Exception {
		try {
			URI u = new URI(p.getProperty("input"));
			SimpleDbCommitter.commitNewAttribute(db, dom, u.toString(), attr, "1");
			sqs.deleteMessage(new DeleteMessageRequest(workQueue, p.getReceiptHandle()));
		} finally {
			finalWork(p);
		}
	}

	@Override
	public WorkUnit getNextWorkUnit() throws Exception {
		WorkUnit out = null;
		ReceiveMessageResult r = sqs.receiveMessage(new ReceiveMessageRequest(workQueue));
		for (final Message m : r.getMessages()) {
			if (out != null) {
				throw new IllegalStateException();
			}

			final String rh = m.getReceiptHandle();

			ScheduledFuture<?> keepAlive = ses.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					sqs.changeMessageVisibility(new ChangeMessageVisibilityRequest(workQueue, rh, timeout));
				}
			}, timeout / 2, timeout, TimeUnit.SECONDS);

			out = new WorkUnit(SimpleMapReduce.marshall(m.getBody()), rh, keepAlive);
		}

		return out;
	}

	@Override
	public void releaseWork(WorkUnit work) throws Exception {
		finalWork(work);
	}

	protected void finalWork(WorkUnit work) throws Exception {
		work.getKeepAlive().cancel(false);
	}

	@Override
	public List<IInputFile> getInputFilesForWorkUnit(WorkUnit p) throws Exception {

		final List<IInputFile> out = new LinkedList<IInputFile>();

		final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		df.setTimeZone(TimeZone.getTimeZone("GMT"));

		URI in = new URI(p.getProperty("input"));

		final String bucket = SMRDriver.extractBucket(in);
		final String key = SMRDriver.extractKey(in);

		AWSUtils.scanObjectsInBucket(s3, bucket, key, new IBucketListener() {

			@Override
			public boolean add(S3ObjectSummary s) throws Exception {

				final URI u = new URI("s3://" + bucket + "/" + s.getKey());
				final String lm = df.format(s.getLastModified());
				final long size = s.getSize();

				out.add(new IInputFile() {

					@Override
					public URI getURI() {
						return u;
					}

					@Override
					public long getSize() {
						return size;
					}

					@Override
					public String getLastModified() {
						return lm;
					}
				});
				return true;
			}

			@Override
			public void done() {
			}

		});

		return out;
	}

	@Override
	public void putResultFile(String key, File f) throws Exception {
		s3.putObject(outputBucket, key, f);
	}

}
