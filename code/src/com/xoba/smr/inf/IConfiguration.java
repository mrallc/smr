package com.xoba.smr.inf;

import java.util.List;
import java.util.Properties;

public interface IConfiguration {

	public Properties getProperties();

	public List<IConfiguration> getChildren();

}