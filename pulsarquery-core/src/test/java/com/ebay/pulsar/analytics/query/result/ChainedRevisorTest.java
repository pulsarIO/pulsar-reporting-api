/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.result;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class ChainedRevisorTest {
	@Test
	public void testChainedRevisor(){
		ResultNode node = new ResultNode("testDim", "testDim");
		node.setName("testDim");
		node.setValue("testDim");
		List<String> dimensions = new ArrayList<String>();
		Map<String, String> nameAliasMap = new HashMap<String, String>();
		dimensions.add("testDim");
		nameAliasMap.put("testDim", "dimension");
		ColumnNameRevisor cr = new ColumnNameRevisor(dimensions, nameAliasMap);
		cr.revise(node);
		List<ColumnNameRevisor> revisorList =new ArrayList<ColumnNameRevisor>();
		revisorList.add(cr);
		ChainedRevisor revisor=new ChainedRevisor(revisorList);
		ChainedRevisor revisor2=new ChainedRevisor(cr);
		revisor.revise(node);
		revisor2.revise(node);
		assertTrue("dimension".equals(node.getName()));
	}

}
