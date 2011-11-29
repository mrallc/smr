package com.xoba.smr.be;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Properties;

public interface IBackend<T extends Properties> {

	/**
	 * releaseWork must be called eventually, whether or not commitWork is
	 * called
	 */
	public T getNextWorkUnit() throws Exception;

	public void commitWork(T work) throws Exception;

	public void releaseWork(T work) throws Exception;

	public List<IInputFile> getInputFilesForWorkUnit(T work) throws Exception;

	public void getInputFile(URI u, File f) throws Exception;

	public void putResultFile(String key, File f) throws Exception;

	public boolean isDone() throws Exception;

	public void sendNotification(String type) throws Exception;

}