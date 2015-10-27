/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import static org.junit.Assert.*;

import org.junit.Test;

public class PulsarDataSourceRuleTest {
	@Test
	public void testPulsarDataSourceRule() {
		PulsarDataSourceRule rule = new PulsarDataSourceRule("druid", "day");
		rule.setDatasourceType("druid");
		rule.setPeriod("day");
		PulsarDataSourceRule rule1 = new PulsarDataSourceRule("druid", "day");
		rule1.setDatasourceType("druid");
		rule1.setPeriod("day");
		assertEquals("druid",rule.getDatasourceType());
		assertEquals("day",rule.getPeriod());
		assertEquals(rule.hashCode(),rule1.hashCode());
		PulsarDataSourceRule rule2 = new PulsarDataSourceRule(null, "day");
		rule2.setPeriod("day");
		PulsarDataSourceRule rule3 = new PulsarDataSourceRule("druid", null);
		rule3.setDatasourceType("druid");
		PulsarDataSourceRule rule4 = new PulsarDataSourceRule("druid", "day");
		rule4.setDatasourceType("druid");
		rule4.setPeriod("week");
		PulsarDataSourceRule rule5 = new PulsarDataSourceRule("druid", "day");
		rule5.setDatasourceType("druid1");
		rule5.setPeriod("day");
		assertFalse(rule2.equals(rule));
		assertFalse(rule3.equals(rule));
		assertFalse(rule4.equals(rule));
		assertFalse(rule5.equals(rule));

	}
}
