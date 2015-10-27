/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.TableDimension;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.metricstore.db.ReflectFieldUtil;
import com.ebay.pulsar.analytics.query.sql.FilterHavingParser;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class FilterHavingParserTest extends FilterHavingParser{
	@Test
	public void testFilterHavingParser() {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable group by _cpgnname having gmv >0 limit 300";

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

		Set<String> aggregateSet = new HashSet<String>();
		aggregateSet.add("site");
		aggregateSet.add("gmv");

		this.setColumnCheck(false);
		QueryWhereHaving queryDesc = parse(sql, table1, aggregateSet);
		assertTrue(queryDesc.getHavingClauses().size() > 0);

	}

	@Test
	public void testFilterHavingParserCheck() throws Exception {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable group by _cpgnname having gmv >0 limit 300";

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
		Map<String, TableDimension> dimMetaMap = Maps.newHashMap();
		dimMetaMap.put("gmv", dimension);
		ReflectFieldUtil
				.setField(Table.class, table1, "dimMetaMap", dimMetaMap);

		//Set<String> aggregateSet = new HashSet<String>();
		// aggregateSet.add("site");
		// aggregateSet.add("gmv");

		this.setColumnCheck(true);
		try {
			parse(sql, table1);
			fail("Expect SqlTranslationException!");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}

	}
	
	@Test
	public void testFilterHavingParserFailed() throws Exception {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable group by _cpgnname having sum(gmv_ag) >0 limit 300";

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
		Map<String, TableDimension> dimMetaMap = Maps.newHashMap();
		dimMetaMap.put("gmv", dimension);
		ReflectFieldUtil
				.setField(Table.class, table1, "dimMetaMap", dimMetaMap);

		//Set<String> aggregateSet = new HashSet<String>();
		// aggregateSet.add("site");
		// aggregateSet.add("gmv");

		this.setColumnCheck(true);
		try {
			parse(sql, table1);
			fail("Expect SqlTranslationException!");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}

	}
	
	
	@Test
	public void testQueryWhereHaving() throws Exception {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable group by _cpgnname having 0<gmv limit 300";

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

		Set<String> aggregateSet = new HashSet<String>();
		aggregateSet.add("site");
		aggregateSet.add("gmv");

		this.setColumnCheck(false);

		try {
			parse(sql, table1, aggregateSet);

		} catch (SqlTranslationException ex) {
			assertTrue(false);
			fail("Expect SqlTranslationException!");
		}

	}
	
	@Test
	public void testQueryWhereHavingFailed() throws Exception {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable  group by _cpgnname having 0<1 limit 300";

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

		Set<String> aggregateSet = new HashSet<String>();
		aggregateSet.add("site");
		aggregateSet.add("gmv");

		this.setColumnCheck(false);

		try {
			parse(sql, table1, aggregateSet);
			fail("Expect SqlTranslationException!");
		} catch (SqlTranslationException ex) {
			assertTrue(true);

		}

	}
	
	@Test
	public void testQueryWhereHavingColumnCheck() throws Exception {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable  group by _cpgnname having gmv_ag>0 limit 300";

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

		//Set<String> aggregateSet = new HashSet<String>();

		this.setColumnCheck(true);

		try {
			parse(sql, table1);
			fail("Expect SqlTranslationException!");
		} catch (SqlTranslationException ex) {
			assertTrue(true);

		}

	}
	
	@Test
	public void testQueryWhereHavingColumnCheckFailed() throws Exception {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable  group by _cpgnname having gmv_ag>0 limit 300";

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

		Set<String> aggregateSet = new HashSet<String>();
		aggregateSet.add("site");
		aggregateSet.add("gmv");

		this.setColumnCheck(false);

		try {
			parse(sql, table1, aggregateSet);
			fail("Expect SqlTranslationException!");
		} catch (SqlTranslationException ex) {
			assertTrue(true);

		}

	}
	
	@Test
	public void testQueryWhereHavingColumnLike() throws Exception {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable where _cpgnname like 'a%' limit 300";
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

		Set<String> aggregateSet = new HashSet<String>();
		aggregateSet.add("site");
		aggregateSet.add("gmv");

		this.setColumnCheck(false);

		try {
			parse(sql, table1, aggregateSet);

		} catch (SqlTranslationException ex) {
			assertTrue(false);
			fail("Expect SqlTranslationException!");
		}

	}
	
	@Test
	public void testQueryWhereHavingFailed3() throws Exception {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable group by _cpgnname having gmv<gmv limit 300";
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

		//Set<String> aggregateSet = new HashSet<String>();

		this.setColumnCheck(true);

		try {
			parse(sql, table1);
			fail("Expect SqlTranslationException!");
		} catch (SqlTranslationException ex) {
			assertTrue(true);

		}

	}
	

	
}
