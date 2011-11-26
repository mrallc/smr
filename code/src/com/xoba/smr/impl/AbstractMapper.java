package com.xoba.smr.impl;

import com.xoba.smr.inf.IMapper;
import com.xoba.smr.inf.IMappingContext;
import com.xoba.util.ILogger;
import com.xoba.util.LogFactory;

public abstract class AbstractMapper implements IMapper {

	private static final ILogger logger = LogFactory.getDefault().create();

	@Override
	public void beginContext(IMappingContext context) {
		logger.debugf("mapping %s, created %s", context.getInputSplit().getURI(), context.getInputSplit()
				.getLastModified());
	}

}
