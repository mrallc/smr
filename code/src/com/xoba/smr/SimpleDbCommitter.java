package com.xoba.smr;

import java.util.Collections;

import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.UpdateCondition;

public class SimpleDbCommitter {

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

}
