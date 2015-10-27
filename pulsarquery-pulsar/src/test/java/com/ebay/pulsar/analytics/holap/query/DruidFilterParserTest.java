/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.holap.query;



import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.TableDimension;
import com.ebay.pulsar.analytics.holap.query.DruidFilterParser;
import com.google.common.collect.Lists;

public class DruidFilterParserTest {
	@Test
	public void testDruidFilter(){
		String sql = " _cpgnname like 'a%' limit 300";

		TableDimension dimension = new TableDimension();
		dimension.setName("gmv_ag");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("gmv_ag", dimension.getName());
		assertEquals(0, dimension.getType());

		TableDimension dimension2 = new TableDimension();
		dimension2.setName("_cpgnname");
		dimension2.setType(0);
		dimension2.setMultiValue(true);
		assertEquals("_cpgnname", dimension2.getName());
		assertEquals(0, dimension2.getType());

		TableDimension dimension3 = new TableDimension();
		dimension3.setName("site");
		dimension3.setType(0);
		dimension3.setMultiValue(true);
		assertEquals("site", dimension3.getName());
		assertEquals(0, dimension3.getType());

		List<TableDimension> dimensions = new ArrayList<TableDimension>();
		dimensions.add(dimension);
		dimensions.add(dimension2);
		dimensions.add(dimension3);

		TableDimension metric = new TableDimension();
		metric.setName("sum");
		metric.setType(0);
		metric.setMultiValue(true);
		assertEquals("sum", metric.getName());
		assertEquals(0, metric.getType());

		Table table1 = new Table();
		table1.setTableName("testTable");
		table1.setDimensions(dimensions);
		table1.setNoInnerJoin(false);
		table1.setDateColumn("testDate");
		table1.setMetrics(Lists.newArrayList(metric));
		DruidFilterParser paser=new DruidFilterParser();
		assertEquals("regex",paser.parseWhere(sql, table1).getType().toString());
	}

}
