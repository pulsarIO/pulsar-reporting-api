/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.result;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class HllMetricRevisorTest {
	@Test
	public void testHllMetricRevisor(){
		ResultNode node = new ResultNode("testDim", "testDim");
		node.setName("testDim");
		node.setValue(10);
		Set<String> hllSet=new HashSet<String>();
		hllSet.add("testDim");
		HllMetricRevisor revisor=new HllMetricRevisor(hllSet);
		revisor.revise(node);
		assertTrue(10L==(Long)node.getValue());
	}
}
