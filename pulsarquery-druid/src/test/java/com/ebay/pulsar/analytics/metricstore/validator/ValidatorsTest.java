/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.validator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.DruidGranularityParser;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.PeriodGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.query.validator.DruidGranularityValidator;
import com.ebay.pulsar.analytics.metricstore.druid.query.validator.GranularityAndTimeRange;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.ebay.pulsar.analytics.query.validator.QueryValidator;
public class ValidatorsTest {

	@Before
	public void setup() throws Exception {
	}

	@After
	public void after() throws Exception {
	}

	@Test
	public void test() {
		testDruidGranularityValidator();
		testGranularityAndTimeRange();
	}
	private void testDruidGranularityValidator(){
		BaseGranularity g1=new PeriodGranularity("PT20M");
		DateTime startTime=DateTime.parse("2015-09-9T23:59:59");
		DateTime endTime=DateTime.parse("2015-09-15T23:59:59");
		DateRange tr = new DateRange(startTime, endTime);
		GranularityAndTimeRange c1=new GranularityAndTimeRange(g1,tr);
		QueryValidator<GranularityAndTimeRange> validator=new DruidGranularityValidator();
		try{
			validator.validate(c1);
			assertTrue(true);
		}catch(Exception e){
			fail("Should be pass.");
		}
		try{
			g1=DruidGranularityParser.parse("PT20Madsfasfa");
			c1=new GranularityAndTimeRange(g1,tr);
			validator.validate(c1);
			fail("Invalid Granularity.");
		}catch(InvalidQueryParameterException e){
			assertTrue(true);
		}
		
		try{
			g1=new PeriodGranularity("PT1M");
			startTime=DateTime.parse("2015-09-9T23:59:59");
			endTime=DateTime.parse("2015-09-15T23:59:59");
			tr = new DateRange(startTime, endTime);
			c1=new GranularityAndTimeRange(g1,tr);
			validator.validate(c1);
			fail("Invalid Invalid Granularity Interval.");
		}catch(InvalidQueryParameterException e){
			assertTrue(true);
		}
	}
	private void testGranularityAndTimeRange(){
		BaseGranularity g1=new PeriodGranularity("PT20M");
		DateTime startTime=DateTime.parse("2015-09-9T23:59:59");
		DateTime endTime=DateTime.parse("2015-09-15T23:59:59");
		DateRange tr = new DateRange(startTime, endTime);
		GranularityAndTimeRange c1=new GranularityAndTimeRange(g1,tr);
		
		assertEquals(g1,c1.getGranularity());
		assertEquals(tr,c1.getIntervals());
		
		assertTrue(c1.equals(c1));
		assertTrue(!c1.equals(null));
		GranularityAndTimeRange c2=new GranularityAndTimeRange(null,null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new GranularityAndTimeRange(g1,null);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new GranularityAndTimeRange(null,tr);
		assertTrue(!c1.equals(c2));assertTrue(!c2.equals(c1));
		c2=new GranularityAndTimeRange(g1,tr);
		assertTrue(c1.equals(c2));assertTrue(c2.equals(c1));
		assertTrue(c1.hashCode()==c2.hashCode());
		
		assertTrue(!c1.equals(new Object(){}));
		
	}
}
