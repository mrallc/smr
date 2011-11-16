package com.xoba.smr.inf;

public interface IMappingContext {

	/**
	 * iso8601 time of when input was last modified
	 */
	public String getLastModified();

	/**
	 * size of input in bytes
	 */
	public Long getSize();

	/**
	 * name of input; e.g., its uri or filename
	 */
	public String getName();

}
