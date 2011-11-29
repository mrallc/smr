package com.xoba.smr;

import java.io.StringWriter;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.util.json.JSONWriter;
import com.xoba.smr.inf.IConfiguration;

public class MapReduceConfig implements IConfiguration {

	private Properties properties;
	private final List<IConfiguration> children = new LinkedList<IConfiguration>();

	public static String serialize(IConfiguration tree) throws Exception {
		StringWriter sw = new StringWriter();
		JSONWriter jw = new JSONWriter(sw);
		add(tree, jw);
		sw.close();
		return sw.toString();
	}

	public static IConfiguration marshall(String s) throws Exception {
		return marshall(new ObjectMapper().readValue(s, HashMap.class));
	}

	private static void add(IConfiguration tree, JSONWriter jw) throws Exception {
		jw.object();
		jw.key("properties");

		jw.object();
		Properties p = tree.getProperties();
		for (Object o : p.keySet()) {
			jw.key(o.toString()).value(p.get(o));
		}
		jw.endObject();

		List<IConfiguration> children = tree.getChildren();
		if (children.size() > 0) {
			jw.key("children");
			jw.array();
			for (IConfiguration c : children) {
				add(c, jw);
			}
			jw.endArray();
		}
		jw.endObject();

	}

	@SuppressWarnings("all")
	private static IConfiguration marshall(Map map) {
		MapReduceConfig ct = new MapReduceConfig();
		if (map.containsKey("properties")) {
			Map propertiesMap = (Map) map.get("properties");
			Properties p = new Properties();
			for (Object o : propertiesMap.keySet()) {
				p.put(o.toString(), propertiesMap.get(o).toString());
			}
			ct.setProperties(p);
		}
		if (map.containsKey("children")) {
			for (Object o : (List) map.get("children")) {
				ct.add(marshall((Map) o));
			}
		}
		return ct;
	}

	@Override
	public String toString() {
		return new Formatter().format("{properties=%s;children=%s}", properties, children).toString();
	}

	public MapReduceConfig add(IConfiguration t) {
		children.add(t);
		return this;
	}

	public MapReduceConfig setProperties(Properties properties) {
		this.properties = properties;
		return this;
	}

	@Override
	public Properties getProperties() {
		return properties;
	}

	@Override
	public List<IConfiguration> getChildren() {
		return Collections.unmodifiableList(children);
	}

}
