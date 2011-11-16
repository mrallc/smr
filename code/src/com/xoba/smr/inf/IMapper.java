package com.xoba.smr.inf;

public interface IMapper {

	/**
	 * sets the context for subsequent keys and values
	 */
	public void beginContext(IMappingContext context);

	public void map(byte[] key, byte[] value, ICollector output) throws Exception;

}
