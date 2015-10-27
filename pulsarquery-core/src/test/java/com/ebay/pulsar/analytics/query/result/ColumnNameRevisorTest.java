/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.result;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class ColumnNameRevisorTest {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testColumn() {
		ResultNode node = new ResultNode("testDim", "testDim");
		node.setName("testDim");
		node.setValue("testDim");
		assertTrue("testDim".equals(node.getName()));
		assertTrue("testDim".equals(node.getValue()));

		List<String> dimensions = new ArrayList<String>();
		Map<String, String> nameAliasMap = new HashMap<String, String>();
		dimensions.add("testDim");
		nameAliasMap.put("testDim", "dimension");
		ColumnNameRevisor cr = new ColumnNameRevisor(dimensions, nameAliasMap);
		cr.revise(node);
		assertTrue("dimension".equals(node.getName()));

		ColumnValueCollector cc = new ColumnValueCollector("dimension");
		List<String> valueList = new ArrayList<String>();
		ColumnValueCollector cc2 = new ColumnValueCollector("dimension",
				valueList);
		assertTrue(valueList.equals(cc2.getValueCollection()));
		valueList.add("testDim");
		cc.revise(node);
		cc2.revise(node);
		assertTrue(valueList.equals(cc2.getValueCollection()));
	}

}
