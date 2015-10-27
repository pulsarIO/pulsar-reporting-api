/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Test;

import com.ebay.pulsar.analytics.query.request.DateRange;

public class PulsarDataSourceRoutingStrategyTest {
	@Test
	public void testDataSourceRouting(){
		PulsarDataSourceRoutingStrategy strategy=new PulsarDataSourceRoutingStrategy();
		PulsarDataSourceRule rule = new PulsarDataSourceRule("druid", "P3M");
		rule.setDatasourceType("druid");
		rule.setPeriod("P3M");
		Map<String ,PulsarDataSourceRule> configuration=new HashMap<String ,PulsarDataSourceRule>();
		configuration.put("event", rule);
		strategy.setConfiguration(configuration);
		assertEquals(configuration,strategy.getConfiguration());
		DateTime startTime=DateTime.parse("2015-09-15T23:59:59");
		DateTime endTime=DateTime.parse("2015-09-15T23:59:59");
		DateRange dataRange = new DateRange(startTime, endTime);	
		assertEquals(strategy.getDataSourceName("event", dataRange, "tracking", "trackingdruid"),"trackingdruid");
	}

}
