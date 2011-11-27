package com.xoba.smr;

import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.amazonaws.services.simpledb.model.UpdateCondition;

public class SimpleDbCommitter {

	/**
	 * note: if exception is thrown, task might be "committed" but not actually
	 * executed.
	 */
	public static <T> T tryToRunIdempotentTaskOnce(AmazonSimpleDB db, String dom, String itemName, String attrName,
			Callable<T> task) throws Exception {
		if (SimpleDbCommitter.countCommitted(db, dom, attrName) < 1) {
			SimpleDbCommitter.commitNewAttribute(db, dom, itemName, attrName, "1");
			return task.call();
		}
		return null;
	}

	/**
	 * only succeeds if attribute is new
	 */
	public static void commitNewAttribute(AmazonSimpleDB db, String dom, String item, String attrName, String attrValue)
			throws Exception {
		PutAttributesRequest req = new PutAttributesRequest();
		req.withDomainName(dom);
		req.withItemName(item);
		req.withAttributes(Collections.singletonList((new ReplaceableAttribute().withName(attrName)
				.withValue(attrValue))));
		req.withExpected(new UpdateCondition(attrName, null, false));
		db.putAttributes(req);
	}

	public static long countCommitted(AmazonSimpleDB db, final String dom, String countAttr) throws Exception {

		long foundCommitted = 0;
		String expr = new Formatter().format("select %s from %s where %s is not null", countAttr, dom, countAttr)
				.toString();
		SelectResult sr = db.select(new SelectRequest(expr, true));
		boolean done = false;
		while (!done) {
			List<Item> items = sr.getItems();
			for (Item i : items) {
				Map<String, String> map = new HashMap<String, String>();
				for (Attribute a : i.getAttributes()) {
					map.put(a.getName(), a.getValue());
				}
				if (map.containsKey(countAttr)) {
					foundCommitted += new Long(map.get(countAttr));
				}
			}
			String t = sr.getNextToken();
			if (t == null) {
				done = true;
			} else {
				SelectRequest req = new SelectRequest(expr, true);
				req.setNextToken(t);
				sr = db.select(req);
			}
		}

		return foundCommitted;
	}

	public static boolean isDone(AmazonSimpleDB db, final String dom, String item, String targetAttr, String countAttr)
			throws Exception {

		Long splits = null;

		GetAttributesResult gar = db.getAttributes(new GetAttributesRequest(dom, item).withAttributeNames(targetAttr));
		for (Attribute a : gar.getAttributes()) {
			if (a.getName().equals(targetAttr)) {
				splits = new Long(a.getValue());
			}
		}

		if (splits == null) {
			return false;
		}

		return splits.equals(countCommitted(db, dom, countAttr));

	}

}
