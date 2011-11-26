package com.xoba.smr.be;

import java.net.URI;

public interface IInputFile {

	public URI getURI();

	public long getSize();

	public String getLastModified();
}