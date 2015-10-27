/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.granularity;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.DruidGranularityParser;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.DurationGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.InvalidGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.PeriodGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.SimpleGranularity;
public class GranularityTest {
	@Test
	public void test() {
		testDruidGranularityParser();
		testDruidGranularity();
		testInvalidGranularity();
		testPeriodGranularity();
		testSimpleGranularity();
	}
	private void testDruidGranularityParser(){
		BaseGranularity c1=DruidGranularityParser.parse(null);
		BaseGranularity c2=DruidGranularityParser.parse("all");
		assertEquals(c1, BaseGranularity.ALL);assertEquals(c1, c2);
		
		BaseGranularity c3=DruidGranularityParser.parse("thirty_minute");
		BaseGranularity c30=DruidGranularityParser.parse("PT30M");
		assertEquals(c3, c30);
		
		String DEFAULT_ORIGIN1 = "1970-01-04T00:00:00-07:00";
		String DEFAULT_ORIGIN2 = "1970-01-01T00:00:00-07:00";
		BaseGranularity c4=DruidGranularityParser.parse("PT20M");
		assertTrue(c4 instanceof PeriodGranularity);
		assertEquals("period",((PeriodGranularity)c4).getType());
		assertEquals("MST",((PeriodGranularity)c4).getTimeZone());
		assertEquals("PT20M",((PeriodGranularity)c4).getPeriod());
		assertEquals(DEFAULT_ORIGIN1,((PeriodGranularity)c4).getOrigin());
		
		BaseGranularity c5=DruidGranularityParser.parse("month");
		assertEquals(DEFAULT_ORIGIN2,((PeriodGranularity)c5).getOrigin());
		
		BaseGranularity c6=DruidGranularityParser.parse("thirty_minute2");
		assertEquals(new InvalidGranularity("thirty_minute2"),c6);
	}
	private void testDruidGranularity(){
		String d1="7200000";
		String origin1="1970-01-01T00:07:00Z";
		DurationGranularity c1 = new DurationGranularity(d1);
		DurationGranularity c2 = new DurationGranularity(d1);

		assertArrayEquals(c1.cacheKey(), c2.cacheKey());
		c1 = new DurationGranularity(d1,origin1);
		c2 = new DurationGranularity(d1,origin1);
		assertArrayEquals(c1.cacheKey(), c2.cacheKey());
		assertEquals(d1,c2.getDuration());
		assertEquals(origin1,c2.getOrigin());
		assertEquals("duration",c2.getType());
		
		c2 = new DurationGranularity(d1);
		assertTrue(!DigestUtils.shaHex(c1.cacheKey()).equals(DigestUtils.shaHex(c2.cacheKey())));
		c2 = new DurationGranularity("7300000",origin1);
		assertTrue(!DigestUtils.shaHex(c1.cacheKey()).equals(DigestUtils.shaHex(c2.cacheKey())));
		c2 = new DurationGranularity("7200000","1970-01-02T00:07:00Z");
		assertTrue(!DigestUtils.shaHex(c1.cacheKey()).equals(DigestUtils.shaHex(c2.cacheKey())));

		
		assertTrue(!c1.equals(null));
		c2 = new DurationGranularity(null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2 = new DurationGranularity(d1);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2 = new DurationGranularity(d1,null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2 = new DurationGranularity(d1,origin1);
		assertTrue(c1.equals(c2));assertTrue(c2.equals(c1));
		
		assertTrue(c1.equals(c1));
		assertTrue(c1.hashCode()==c2.hashCode());
		assertTrue(!c1.equals(new Object()));
	}
	
	private void testInvalidGranularity(){
		String key="abc";
		InvalidGranularity c1=new InvalidGranularity(key);
		assertArrayEquals(c1.cacheKey(), new InvalidGranularity(key).cacheKey());
		assertEquals(key, c1.getKey());
		
		InvalidGranularity c2=new InvalidGranularity(null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new InvalidGranularity("hour");
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		
		assertTrue(c1.equals(c1));
		assertTrue(!c1.equals(null));
		assertTrue(!c1.equals(new Object(){}));
	}
	private void testPeriodGranularity(){
		String d1="P2D";
		String timeZone="UTC";
		String origin1="1970-01-01T00:07:00Z";
		PeriodGranularity c1 = new PeriodGranularity(d1,timeZone);
		PeriodGranularity c2 = new PeriodGranularity(d1,timeZone);

		assertArrayEquals(c1.cacheKey(), c2.cacheKey());
		c1 = new PeriodGranularity(d1,timeZone,origin1);
		c2 = new PeriodGranularity(d1,timeZone,origin1);
		assertArrayEquals(c1.cacheKey(), c2.cacheKey());
		assertEquals(d1,c2.getPeriod());
		assertEquals(origin1,c2.getOrigin());
		assertEquals(timeZone,c2.getTimeZone());
		assertEquals("period",c2.getType());
		
		c2 = new PeriodGranularity(d1);
		assertTrue(!DigestUtils.shaHex(c1.cacheKey()).equals(DigestUtils.shaHex(c2.cacheKey())));
		c2 = new PeriodGranularity(d1,"MST",origin1);
		assertTrue(!DigestUtils.shaHex(c1.cacheKey()).equals(DigestUtils.shaHex(c2.cacheKey())));
		c2 = new PeriodGranularity(d1,"MST","1970-01-02T00:07:00Z");
		assertTrue(!DigestUtils.shaHex(c1.cacheKey()).equals(DigestUtils.shaHex(c2.cacheKey())));

		
		assertTrue(!c1.equals(null));
		c2 = new PeriodGranularity(null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2 = new PeriodGranularity(d1);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2 = new PeriodGranularity(d1,null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2 = new PeriodGranularity(d1,timeZone,null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2 = new PeriodGranularity(null,null,null);
		assertTrue(!c2.equals(c1));
		c2 = new PeriodGranularity(null,null,origin1);
		assertTrue(!c2.equals(c1));
		c2 = new PeriodGranularity(d1,null,origin1);
		assertTrue(!c2.equals(c1));
		c2 = new PeriodGranularity(null,timeZone,origin1);
		assertTrue(!c2.equals(c1));
		c2 = new PeriodGranularity("P3D",timeZone,origin1);
		assertTrue(!c2.equals(c1));
		c2 = new PeriodGranularity(d1,"MST",origin1);
		assertTrue(!c2.equals(c1));
		
		c2 = new PeriodGranularity(d1,timeZone,origin1);
		assertTrue(c1.equals(c2));assertTrue(c2.equals(c1));
		
		
		
		assertTrue(c1.equals(c1));
		assertTrue(c1.hashCode()==c2.hashCode());
		assertTrue(!c1.equals(new Object()));
	}
	private void testSimpleGranularity(){
		String key="all";
		SimpleGranularity c1=new SimpleGranularity(key);
		assertArrayEquals(c1.cacheKey(), BaseGranularity.ALL.cacheKey());
		assertEquals(key, c1.getKey());
		
		SimpleGranularity c2=new SimpleGranularity(null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new SimpleGranularity("hour");
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new SimpleGranularity("all");
		assertTrue(c1.hashCode()==c2.hashCode());
		assertTrue(c1.equals(c1));
		assertTrue(!c1.equals(null));
		assertTrue(!c1.equals(new Object(){}));
	}

}
