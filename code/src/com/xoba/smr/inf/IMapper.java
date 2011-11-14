package com.xoba.smr.inf;

public interface IMapper {

	public void map(byte[] key, byte[] value, ICollector output) throws Exception;

}
