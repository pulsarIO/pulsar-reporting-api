/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Lists;

public class TableTest {
	@Test
	public void testEquals(){
		TableDimension dimension=new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim",dimension.getName());
		assertEquals(0,dimension.getType());
		
		TableDimension dimension2=new TableDimension("testDim",0);
		dimension2.setName("testDim");
		dimension2.setType(0);
		dimension2.setMultiValue(true);
		
		TableDimension dimension3=new TableDimension();
		dimension3.setType(0);
		dimension3.setMultiValue(true);
		
		TableDimension dimension4=new TableDimension();
		dimension4.setName("testDim");
		dimension4.setType(1);
		dimension4.setMultiValue(true);
		
		TableDimension dimension5=new TableDimension();
		dimension5.setName("testDim");
		dimension5.setType(0);
		dimension5.setMultiValue(false);
		assertTrue(dimension2.equals(dimension));
		assertFalse(dimension3.equals(dimension));
		assertFalse(dimension4.equals(dimension));
		assertFalse(dimension5.equals(dimension));
		assertTrue(dimension.hashCode()==dimension2.hashCode());
		
		Table table1 = new Table();
		table1.setTableName("testTable");
		table1.setDimensions(Lists.newArrayList(dimension));
		table1.setNoInnerJoin(false);
		table1.setDateColumn("testDate");
		table1.setMetrics(Lists.newArrayList(dimension));
		table1.insertDimensionMap(dimension);
		table1.insertMetricMap(dimension);
		
		Table table2 = new Table();
		table2.setTableName("testTable");
		table2.setDimensions(Lists.newArrayList(dimension));
		table2.setNoInnerJoin(false);
		table2.setDateColumn("testDate");
		table2.setMetrics(Lists.newArrayList(dimension));
		table2.insertDimensionMap(dimension);
		table2.insertMetricMap(dimension);	
		
		Table table3 = new Table();
		table3.setDimensions(Lists.newArrayList(dimension));
		table3.setNoInnerJoin(false);
		table3.setDateColumn("testDate");
		table3.setMetrics(Lists.newArrayList(dimension));
		table3.insertDimensionMap(dimension);
		table3.insertMetricMap(dimension);
		
		Table table4 = new Table();
		table4.setTableName("testTable");
		table4.setNoInnerJoin(false);
		table4.setDateColumn("testDate");
		table4.setMetrics(Lists.newArrayList(dimension));
		table4.insertDimensionMap(dimension);
		table4.insertMetricMap(dimension);
		
		Table table5 = new Table();
		table5.setTableName("testTable");
		table5.setNoInnerJoin(true);
		table5.setDimensions(Lists.newArrayList(dimension));
		table5.setDateColumn("testDate");
		table5.setMetrics(Lists.newArrayList(dimension));
		table5.insertDimensionMap(dimension);
		table5.insertMetricMap(dimension);
		
		Table table6 = new Table();
		table6.setTableName("testTable");
		table6.setDimensions(Lists.newArrayList(dimension));
		table6.setNoInnerJoin(false);
		table6.setMetrics(Lists.newArrayList(dimension));
		table6.insertDimensionMap(dimension);
		table6.insertMetricMap(dimension);
		
		Table table7 = new Table();
		table7.setTableName("testTable");
		table7.setDimensions(Lists.newArrayList(dimension));
		table7.setNoInnerJoin(false);
		table7.setDateColumn("testDate");
		table7.insertDimensionMap(dimension);
		table7.insertMetricMap(dimension);
		
		Table table8 = new Table();
		table8.setTableName("testTable8");
		table8.setDimensions(Lists.newArrayList(dimension));
		table8.setNoInnerJoin(false);
		table8.setDateColumn("testDate");
		table8.setMetrics(Lists.newArrayList(dimension));
		table8.insertDimensionMap(dimension);
		table8.insertMetricMap(dimension);
		
		Table table9 = new Table();
		table9.setTableName("testTable");
		table9.setDimensions(Lists.newArrayList(dimension3));
		table9.setNoInnerJoin(false);
		table9.setDateColumn("testDate");
		table9.setMetrics(Lists.newArrayList(dimension));
		table9.insertDimensionMap(dimension);
		table9.insertMetricMap(dimension);
		
		Table table10 = new Table();
		table10.setTableName("testTable");
		table10.setDimensions(Lists.newArrayList(dimension));
		table10.setNoInnerJoin(false);
		table10.setDateColumn("testDate10");
		table10.setMetrics(Lists.newArrayList(dimension));
		table10.insertDimensionMap(dimension);
		table10.insertMetricMap(dimension);
		
		Table table11 = new Table();
		table11.setTableName("testTable");
		table11.setDimensions(Lists.newArrayList(dimension));
		table11.setNoInnerJoin(false);
		table11.setDateColumn("testDate");
		table11.setMetrics(Lists.newArrayList(dimension3));
		table11.insertDimensionMap(dimension);
		table11.insertMetricMap(dimension);
		

		assertTrue(table2.equals(table1));
		assertFalse(table3.equals(table1));
		assertFalse(table4.equals(table1));
		assertFalse(table5.equals(table1));
		assertFalse(table6.equals(table1));
		assertFalse(table7.equals(table1));
		assertFalse(table8.equals(table1));
		assertFalse(table9.equals(table1));
		assertFalse(table10.equals(table1));
		assertFalse(table11.equals(table1));
		assertTrue(table1.hashCode()==table2.hashCode());
		assertTrue(table1.toString().equals(table2.toString()));
		assertTrue(table1.isColumnMetric("testDim"));
		assertFalse(table1.isColumnDouble("testDim"));
		assertFalse(table1.isColumnHyperLogLog("testDim"));
		assertFalse(table1.isColumnNumeric("testDim"));
		assertFalse(table1.isNoInnerJoin());
	}
	
}

