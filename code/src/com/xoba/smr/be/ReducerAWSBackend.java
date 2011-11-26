package com.xoba.smr.be;

import java.util.List;
import java.util.Properties;

import com.xoba.smr.ConfigKey;
import com.xoba.smr.SMRDriver;

public class ReducerAWSBackend extends AbstractBackend {

	public ReducerAWSBackend(Properties c) {
		super(c, ConfigKey.REDUCE_QUEUE, ConfigKey.REDUCE_OUTPUTS_BUCKET);
	}

	@Override
	public void commitWork(WorkUnit p) throws Exception {
		commitWork(p, "reduced");
	}

	@Override
	public boolean isDone() throws Exception {
		return isDone("hashes", "reduced");
	}

	@Override
	public List<IInputFile> getInputFilesForWorkUnit(WorkUnit p) throws Exception {

		List<IInputFile> out = super.getInputFilesForWorkUnit(p);

		if (out.size() != SMRDriver.countCommitted(aws, dom, "mapped")) {
			throw new Exception("missing some map outputs");
		}

		return out;
	}

}
