package com.xoba.smr.be;

import java.util.Properties;

import com.xoba.smr.ConfigKey;

public class MapperAWSBackend extends AbstractBackend {

	public MapperAWSBackend(Properties c) {
		super(c, ConfigKey.MAP_QUEUE, ConfigKey.SHUFFLE_BUCKET);
	}

	@Override
	public void commitWork(WorkUnit p) throws Exception {
		commitWork(p, "mapped");
	}

	@Override
	public boolean isDone() throws Exception {
		return isDone("splits", "mapped");
	}

}
