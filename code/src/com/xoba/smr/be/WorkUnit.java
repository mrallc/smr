package com.xoba.smr.be;

import java.util.Properties;
import java.util.concurrent.ScheduledFuture;

public class WorkUnit extends Properties {

	private final String receiptHandle;
	private final ScheduledFuture<?> keepAlive;

	public WorkUnit(Properties p, String receiptHandle, ScheduledFuture<?> keepAlive) {
		this.putAll(p);
		this.receiptHandle = receiptHandle;
		this.keepAlive = keepAlive;
	}

	public String getReceiptHandle() {
		return receiptHandle;
	}

	public ScheduledFuture<?> getKeepAlive() {
		return keepAlive;
	}

}