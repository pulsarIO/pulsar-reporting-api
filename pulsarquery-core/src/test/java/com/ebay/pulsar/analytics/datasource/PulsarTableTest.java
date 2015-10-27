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

public class PulsarTableTest {
	@Test
	public void testEquals(){
		PulsarTableDimension dimension=new PulsarTableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		dimension.setAlias("Dim");
		dimension.setRTOLAPColumnName("druidColumn");
		dimension.setMOLAPColumnName("kylinColumn");
		assertEquals("testDim",dimension.getName());
		assertEquals(0,dimension.getType());
		assertEquals("druidColumn",dimension.getRTOLAPColumnName());
		assertEquals("kylinColumn",dimension.getMOLAPColumnName());
		assertEquals("Dim",dimension.getAlias());
		
		
		PulsarTableDimension dimension2=new PulsarTableDimension();
		dimension2.setName("testDim");
		dimension2.setType(0);
		dimension2.setMultiValue(true);
		dimension2.setAlias("Dim");
		dimension2.setRTOLAPColumnName("druidColumn");
		dimension2.setMOLAPColumnName("kylinColumn");
		
		PulsarTableDimension dimension3=new PulsarTableDimension();
		dimension3.setName("testDim");
		dimension3.setType(0);
		dimension3.setMultiValue(true);
		dimension3.setRTOLAPColumnName("druidColumn");
		dimension3.setMOLAPColumnName("kylinColumn");
		
		PulsarTableDimension dimension4=new PulsarTableDimension();
		dimension4.setName("testDim");
		dimension4.setType(0);
		dimension4.setMultiValue(true);
		dimension4.setAlias("Dim");
		dimension4.setMOLAPColumnName("kylinColumn");
		
		PulsarTableDimension dimension5=new PulsarTableDimension();
		dimension5.setName("testDim");
		dimension5.setType(0);
		dimension5.setMultiValue(true);
		dimension5.setAlias("Dim");
		dimension5.setRTOLAPColumnName("druidColumn");
		
		PulsarTableDimension dimension6=new PulsarTableDimension();
		dimension6.setName("testDim");
		dimension6.setType(0);
		dimension6.setMultiValue(true);
		dimension6.setAlias("Dim6");
		dimension6.setRTOLAPColumnName("druidColumn");
		dimension6.setMOLAPColumnName("kylinColumn");

		
		PulsarTableDimension dimension7=new PulsarTableDimension();
		dimension7.setName("testDim");
		dimension7.setType(0);
		dimension7.setMultiValue(true);
		dimension7.setAlias("Dim");
		dimension7.setRTOLAPColumnName("druidColumn7");
		dimension7.setMOLAPColumnName("kylinColumn");

		
		PulsarTableDimension dimension8=new PulsarTableDimension();
		dimension8.setName("testDim");
		dimension8.setType(0);
		dimension8.setMultiValue(true);
		dimension8.setAlias("Dim");
		dimension8.setRTOLAPColumnName("druidColumn");
		dimension8.setMOLAPColumnName("kylinColumn8");



		assertTrue(dimension2.equals(dimension));
		assertFalse(dimension3.equals(dimension));
		assertFalse(dimension4.equals(dimension));
		assertFalse(dimension5.equals(dimension));
		assertFalse(dimension6.equals(dimension));
		assertFalse(dimension7.equals(dimension));
		assertFalse(dimension8.equals(dimension));
		assertTrue(dimension.hashCode()==dimension2.hashCode());
		
		PulsarTable table1 = new PulsarTable();
		table1.setTableName("testTable");
		table1.setDimensions(Lists.newArrayList(dimension));
		table1.setNoInnerJoin(false);
		table1.setDateColumn("testDate");
		table1.setMetrics(Lists.newArrayList(dimension));
		table1.insertDimensionMap(dimension);
		table1.insertMetricMap(dimension);
		table1.setRTOLAPTableName("druidTableName");
		table1.setMOLAPTableName("kylinTableName");
		table1.setTableNameAlias("tableNameAlias");
		assertEquals("testTable",table1.getTableName());
		assertEquals(Lists.newArrayList(dimension),table1.getDimensions());
		assertEquals("testDate",table1.getDateColumn());
		assertEquals(Lists.newArrayList(dimension),table1.getMetrics());
		assertEquals("druidTableName",table1.getRTOLAPTableName());
		assertEquals("kylinTableName",table1.getMOLAPTableName());
		assertEquals("tableNameAlias",table1.getTableNameAlias());

		
		PulsarTable table2 = new PulsarTable();
		table2.setTableName("testTable");
		table2.setDimensions(Lists.newArrayList(dimension));
		table2.setNoInnerJoin(false);
		table2.setDateColumn("testDate");
		table2.setMetrics(Lists.newArrayList(dimension));
		table2.insertDimensionMap(dimension);
		table2.insertMetricMap(dimension);
		table2.setRTOLAPTableName("druidTableName");
		table2.setMOLAPTableName("kylinTableName");
		table2.setTableNameAlias("tableNameAlias");
		
		Table table = new Table();
		table.setTableName("testTable");
		table.setDimensions(Lists.newArrayList(dimension));
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setMetrics(Lists.newArrayList(dimension));
		table.insertDimensionMap(dimension);
		table.insertMetricMap(dimension);
		
		PulsarTable table3 = new PulsarTable(table);
		table3.setRTOLAPTableName("druidTabel");
		table3.setMOLAPTableName("kylinTabel");
		table3.setRTOLAPTableName("druidTableName");
		table3.setMOLAPTableName("kylinTableName");
		table3.setTableNameAlias("tableNameAlias");
		
		PulsarTable table4 = new PulsarTable();
		table4.setTableName("testTable");
		table4.setDimensions(Lists.newArrayList(dimension));
		table4.setNoInnerJoin(false);
		table4.setDateColumn("testDate");
		table4.setMetrics(Lists.newArrayList(dimension));
		table4.insertDimensionMap(dimension);
		table4.insertMetricMap(dimension);
		table4.setMOLAPTableName("kylinTableName");
		table4.setTableNameAlias("tableNameAlias");
		
		PulsarTable table5 = new PulsarTable();
		table5.setTableName("testTable");
		table5.setDimensions(Lists.newArrayList(dimension));
		table5.setNoInnerJoin(false);
		table5.setDateColumn("testDate");
		table5.setMetrics(Lists.newArrayList(dimension));
		table5.insertDimensionMap(dimension);
		table5.insertMetricMap(dimension);
		table5.setRTOLAPTableName("druidTableName");
		table5.setTableNameAlias("tableNameAlias");
		
		PulsarTable table6 = new PulsarTable();
		table6.setTableName("testTable");
		table6.setDimensions(Lists.newArrayList(dimension));
		table6.setNoInnerJoin(false);
		table6.setDateColumn("testDate");
		table6.setMetrics(Lists.newArrayList(dimension));
		table6.insertDimensionMap(dimension);
		table6.insertMetricMap(dimension);
		table6.setRTOLAPTableName("druidTableName");
		table6.setMOLAPTableName("kylinTableName");
		
		
		assertTrue(table1.equals(table2));
		assertFalse(table3.equals(table1));
		assertFalse(table4.equals(table1));
		assertFalse(table5.equals(table1));
		assertFalse(table6.equals(table1));
		assertTrue(table1.hashCode()==table2.hashCode());
		assertTrue(table1.isColumnMetric("testDim"));
		assertFalse(table1.isColumnDouble("testDim"));
		assertFalse(table1.isColumnHyperLogLog("testDim"));
		assertFalse(table1.isColumnNumeric("testDim"));
		assertFalse(table1.isNoInnerJoin());
		assertFalse(table1.toString().equals(table3.toString()));
	}
}
