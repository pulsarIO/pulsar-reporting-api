/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.limitspec;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.SortDirection;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.DefaultLimitSpec;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.OrderByColumnSpec;
import com.google.common.collect.Lists;

import static org.junit.Assert.*;
public class LimitSpecTest {

	@Before
	public void setup() throws Exception {
	}

	@After
	public void after() throws Exception {
	}

	@Test
	public void test() {
		testDefaultLimitSpec();
		testOrderByColumnSpec();
	}
	private void testDefaultLimitSpec(){
		String d1="d1";
		OrderByColumnSpec c1=new OrderByColumnSpec(d1,SortDirection.descending);
		DefaultLimitSpec dls=new DefaultLimitSpec(10,Lists.newArrayList(c1));
		
		assertEquals(dls.getColumns(),Lists.newArrayList(c1));
		assertEquals(dls.getLimit(),10);
		assertEquals(dls.getType(),"default");
		assertTrue(dls.equals(dls));
		DefaultLimitSpec dls2=new DefaultLimitSpec(10,Lists.newArrayList(c1));
		assertArrayEquals(dls.cacheKey(),dls2.cacheKey());
		dls2=new DefaultLimitSpec(3,null);
		assertTrue(!dls.equals(dls2));
		assertTrue(!dls2.equals(dls));
		dls2=new DefaultLimitSpec(3,Lists.newArrayList(c1));
		assertTrue(!dls.equals(dls2));
		assertTrue(!dls2.equals(dls));		
		dls2=new DefaultLimitSpec(10,null);
		assertTrue(!dls.equals(dls2));
		assertTrue(!dls2.equals(dls));
		dls2=new DefaultLimitSpec(10,Lists.newArrayList(c1));
		assertTrue(dls.equals(dls2));
		assertTrue(dls2.equals(dls));
		assertTrue(dls2.equals(dls2));
		assertTrue(dls.hashCode()==dls2.hashCode());
		
		assertTrue(!dls.equals(new Object()));
	}
	private void testOrderByColumnSpec(){
		String d1="d1";
		OrderByColumnSpec c1=new OrderByColumnSpec(d1,SortDirection.descending);
		assertEquals(d1,c1.getDimension());
		assertEquals(SortDirection.descending,c1.getDirection());
		String d2="d2";
		OrderByColumnSpec c2=new OrderByColumnSpec(d1,SortDirection.descending);
		assertArrayEquals(c1.cacheKey(),c2.cacheKey());
		c2=new OrderByColumnSpec(d2,SortDirection.descending);
		assertTrue(!DigestUtils.shaHex(c1.cacheKey()).equals(DigestUtils.shaHex(c2.cacheKey())));
		c2=new OrderByColumnSpec(d1,SortDirection.ascending);
		assertTrue(!DigestUtils.shaHex(c1.cacheKey()).equals(DigestUtils.shaHex(c2.cacheKey())));
		
		assertTrue(!c1.equals(null));
		c2=new OrderByColumnSpec(null,null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new OrderByColumnSpec(d2,null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new OrderByColumnSpec(d1,null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new OrderByColumnSpec(d1,SortDirection.ascending);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new OrderByColumnSpec(d1,SortDirection.descending);
		assertTrue(c1.equals(c2));assertTrue(c2.equals(c1));
		assertTrue(c1.hashCode()==c2.hashCode());
	}
}
