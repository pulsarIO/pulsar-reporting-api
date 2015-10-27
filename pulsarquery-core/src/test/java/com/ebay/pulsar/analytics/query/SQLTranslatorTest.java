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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.TableDimension;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.query.sql.SQLTranslator;
import com.ebay.pulsar.analytics.query.sql.SQLTranslator.QueryDescription;
import com.ebay.pulsar.analytics.query.sql.SimpleTableNameParser;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.AggregateNode;
import com.foundationdb.sql.parser.BinaryArithmeticOperatorNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.NumericConstantNode;
import com.foundationdb.sql.parser.SelectNode;
import com.foundationdb.sql.parser.TableName;
import com.google.common.collect.Lists;

public class SQLTranslatorTest{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testGetTableName() {
		String sql = "select count(clickcount_ag) as \"clickcount_ag\", testDim from tabletest group by testDim limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		assertEquals("tabletest", sqlTranslator.getTableName(sql));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testParse() {
		String sql = "select count(clickcount_ag) as \"clickcount_ag\", testdim from tabletest group by testdim limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testdim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testdim", dimension.getName());
		assertEquals(0, dimension.getType());

		TableDimension metric2 = new TableDimension();
		metric2.setName("clickcount_ag");
		metric2.setType(0);
		metric2.setMultiValue(true);

		List<TableDimension> metrics = new ArrayList<TableDimension>();
		metrics.add(metric2);

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		table.setMetrics(metrics);

		assertTrue(sqlTranslator.parseResultList(selectNode, table)
				.getDimensions().contains("testdim"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testParseCount() {
		String sql = "select count(distinct(testdim)) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testdim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testdim", dimension.getName());
		assertEquals(0, dimension.getType());

		TableDimension metric = new TableDimension();
		metric.setName("count");
		metric.setType(0);
		metric.setMultiValue(true);

		TableDimension metric2 = new TableDimension();
		metric2.setName("clickcount_ag");
		metric2.setType(0);
		metric2.setMultiValue(true);

		List<TableDimension> metrics = new ArrayList<TableDimension>();
		metrics.add(metric2);
		metrics.add(metric);

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		table.setMetrics(metrics);

		assertTrue(sqlTranslator.parseResultList(selectNode, table)
				.getSimpleAggregateColsMap().containsKey("testdim"));
		assertTrue(sqlTranslator.parseResultList(selectNode, table)
				.getAggrKeyToAliasMap().containsValue("testdim"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testParseBinary() {
		String sql = "select testdim*2 as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testdim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testdim", dimension.getName());
		assertEquals(0, dimension.getType());

		TableDimension metric = new TableDimension();
		metric.setName("count");
		metric.setType(0);
		metric.setMultiValue(true);

		TableDimension metric2 = new TableDimension();
		metric2.setName("clickcount_ag");
		metric2.setType(0);
		metric2.setMultiValue(true);

		List<TableDimension> metrics = new ArrayList<TableDimension>();
		metrics.add(metric2);
		metrics.add(metric);

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		table.setMetrics(metrics);
		assertTrue(sqlTranslator.parseResultList(selectNode, table)
				.getAggrKeyToAliasMap().containsValue("testdim"));

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testParseOderBy() {
		String sql = "select count(1) as testdim from tabletest where (site=0 or not(region='11')) order by testdim";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testdim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testdim", dimension.getName());
		assertEquals(0, dimension.getType());

		TableDimension metric = new TableDimension();
		metric.setName("count");
		metric.setType(0);
		metric.setMultiValue(true);

		TableDimension metric2 = new TableDimension();
		metric2.setName("clickcount_ag");
		metric2.setType(0);
		metric2.setMultiValue(true);

		List<TableDimension> metrics = new ArrayList<TableDimension>();
		metrics.add(metric2);
		metrics.add(metric);

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		table.setMetrics(metrics);

		assertTrue(sqlTranslator.parseResultList(selectNode, table)
				.getAggrKeyToAliasMap().containsValue("testdim"));

	}
	
	
	@Test
	public void testParseUnsupportError() {
		String sql = "select * from tabletest group by testDim limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		try {
			sqlTranslator.parseResultList(selectNode, table);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}

	}

	@Test
	public void testParseError() {
		String sql = "select count(clickcount_ag) as \"clickcount_ag\", testDim from tabletest group by testDim limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		try {
			sqlTranslator.parseResultList(selectNode, table);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}

	}

	@Test
	public void testParseTableError() {

		String sql = "select count(clickcount_ag) as \"clickcount_ag\", testDim from  group by testDim limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		try {
			sqlTranslator.parse(sql);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@Test
	public void testParseNullTableError() {

		//String sql = "select count(clickcount_ag) as \"clickcount_ag\", testDim from  group by testDim limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		SelectNode selectNode=new SelectNode();
		
		try {
			sqlTranslator.getTableName(selectNode);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@Test
	public void testParseErrorUnsupport() {

		String sql = "select * from  group by testDim limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		try {
			sqlTranslator.parse(sql);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@Test
	public void testParseInvalidTableError() {

		String sql = "select count(clickcount_ag) as \"clickcount_ag\", testDim from tabel1 table2 group by testDim limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		try {
			sqlTranslator.parseResultList(selectNode, table);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}

	}
	
	@Test
	public void testParseAliasError() {

		String sql = "select count(clickcount_ag), testDim from tabel1 table2 group by testDim limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		try {
			sqlTranslator.parseResultList(selectNode, table);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
		String sql2 = "select clickcount_ag*2, testDim from tabel1 table2 group by testDim limit 100";
		QueryDescription queryDesc2 = sqlTranslator.parse(sql2);
		SelectNode selectNode2 = queryDesc2.getSelectNode();
		try {
			sqlTranslator.parseResultList(selectNode2, table);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}

	}

	@Test
	public void testGetInt() {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable group by _cpgnname having sum(gmv_ag) >0 limit 300";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		try {
			sqlTranslator.parseResultList(selectNode, table);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
		NumericConstantNode node = new NumericConstantNode();
		node.setValue(10);
		assertEquals(10, sqlTranslator.getIntValue(node, 10));
		node.setValue(-1);
		try {
			sqlTranslator.getIntValue(node, 10);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}

		node.setValue("10");
		try {
			sqlTranslator.getIntValue(node, 10);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@Test
	public void testSimpleTableNameParser() {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable group by _cpgnname having sum(gmv_ag) >0 limit 300";

		assertTrue("testTable".equals(SimpleTableNameParser.getTableName(sql)));
	}
	
	@Test
	public void testTableNameParserError() {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname  group by _cpgnname having sum(gmv_ag) >0 limit 300";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		try {
			sqlTranslator.getTableName(sql);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@Test
	public void testInvalidTableNameParser() {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from  group by _cpgnname having sum(gmv_ag) >0 limit 300";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		try {
			sqlTranslator.getTableName(sql);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@Test
	public void testGroupByCheckSuccess() {
		String sql = "select count(clickcount_ag) as \"clickcount_ag\", testDim from tabletest group by testDim order by count(clickcount_ag) limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		CursorNode cursorNode = queryDesc.getCursorNode();		
		Map<String, String> columnsMap = new HashMap<String, String>();		
		columnsMap.put("testdim", "testdim");
		
		try {
			sqlTranslator.groupByCheck(selectNode, cursorNode, columnsMap);
		} catch (SqlTranslationException ex) {
			fail("expected SqlTranslationException");
		}
	}
	
	@Test
	public void testGroupByCheck() {
		String sql = "select sum(gmv_ag) as gmv,_cpgnname from testTable group by _cpgnname having sum(gmv_ag) >0 limit 300";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		
		CursorNode cursorNode = queryDesc.getCursorNode();
		Map<String, String> columnsMap = new HashMap<String, String>();
		columnsMap.put("gmv", "gmv");
		try {
			sqlTranslator.groupByCheck(selectNode, cursorNode, columnsMap);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@Test
	public void testGroupByOrderBy() {
		String sql = "select count(clickcount_ag) as \"clickcount_ag\", testDim from tabletest order by count(clickcount_ag) limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		CursorNode cursorNode = queryDesc.getCursorNode();		
		Map<String, String> columnsMap = new HashMap<String, String>();		
		columnsMap.put("testdim", "testdim");
		
		try {
			sqlTranslator.groupByCheck(selectNode, cursorNode, columnsMap);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	

	
	@Test
	public void testGroupByError() {
		String sql = "select count(clickcount_ag) as \"clickcount_ag\", testDim from tabletest group by testDim order by count(clickcount_ag) limit 100";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		QueryDescription queryDesc = sqlTranslator.parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		CursorNode cursorNode = queryDesc.getCursorNode();		
		Map<String, String> columnsMap = new HashMap<String, String>();		
		
		try {
			sqlTranslator.groupByCheck(selectNode, cursorNode, columnsMap);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testInvalidAggregate() {
		String sql = "select count(*) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		AggregateNode node =Mockito.mock(AggregateNode.class);
		Mockito.when(node.getAggregateName()).thenReturn("countall");
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));

		try {
			sqlTranslator.getAggregateKey(node, table,null,true);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testCountAll() {
		String sql = "select count(*) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		AggregateNode node =Mockito.mock(AggregateNode.class);
		Mockito.when(node.getAggregateName()).thenReturn("count(*)");
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));


		assertEquals("countall",sqlTranslator.getAggregateKey(node, table,null,true));

	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetAggregate() {
		String sql = "select count(testdim) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		AggregateNode node =Mockito.mock(AggregateNode.class);
		Mockito.when(node.getAggregateName()).thenReturn("count");
		Mockito.when(node.isDistinct()).thenReturn(true);
		ColumnReference cNode=new ColumnReference();
		TableName name=new TableName();
		cNode.init("test",name);
		Mockito.when(node.getOperand()).thenReturn(cNode);
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));

		try {
			sqlTranslator.getAggregateKey(node, table,null,false);
			
		} catch (SqlTranslationException ex) {
			fail("unexpected SqlTranslationException");
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetAggregateNumericConstantNode() throws StandardException {
		String sql = "select count(testdim) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		AggregateNode node =Mockito.mock(AggregateNode.class);
		Mockito.when(node.getAggregateName()).thenReturn("count");
		Mockito.when(node.isDistinct()).thenReturn(true);
		NumericConstantNode nNode=new NumericConstantNode();
		nNode.init("test",0);
		Mockito.when(node.getOperand()).thenReturn(nNode);
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));

		try {
			sqlTranslator.getAggregateKey(node, table,null,false);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetAggregateNumericConstantNodeNot1() throws StandardException {
		String sql = "select count(testdim) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		AggregateNode node =Mockito.mock(AggregateNode.class);
		Mockito.when(node.getAggregateName()).thenReturn("count");
		Mockito.when(node.isDistinct()).thenReturn(true);
		NumericConstantNode nNode=new NumericConstantNode();
		nNode.init("test",0);
		nNode.setValue(2);
		Mockito.when(node.getOperand()).thenReturn(nNode);
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));

		try {
			sqlTranslator.getAggregateKey(node, table,null,false);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetAggregateNumericConstantNodeNotInt() throws StandardException {
		String sql = "select count(testdim) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		AggregateNode node =Mockito.mock(AggregateNode.class);
		Mockito.when(node.getAggregateName()).thenReturn("count");
		Mockito.when(node.isDistinct()).thenReturn(true);
		NumericConstantNode nNode=new NumericConstantNode();
		nNode.init("test",0);
		nNode.setValue("test");
		Mockito.when(node.getOperand()).thenReturn(nNode);
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));

		try {
			sqlTranslator.getAggregateKey(node, table,null,false);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetUnsupportAggregate() throws StandardException {
		String sql = "select count(testdim) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		AggregateNode node =Mockito.mock(AggregateNode.class);
		Mockito.when(node.getAggregateName()).thenReturn("count");
		Mockito.when(node.isDistinct()).thenReturn(true);

		Mockito.when(node.getOperand()).thenReturn(null);
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));

		try {
			sqlTranslator.getAggregateKey(node, table,null,false);
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testAddCompositeAggregateNode() throws StandardException{
		String sql = "select count(testdim) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		AggregateNode node =Mockito.mock(AggregateNode.class);
		Mockito.when(node.getAggregateName()).thenReturn("count");
		Mockito.when(node.isDistinct()).thenReturn(true);
		NumericConstantNode nNode=new NumericConstantNode();
		nNode.init("test",0);
		nNode.setValue(1);
		Mockito.when(node.getOperand()).thenReturn(nNode);
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		
		Map<String, AggregateNode> aggregateNodesMap =new HashMap<String, AggregateNode>();
		aggregateNodesMap.put("count", node);
		
		Map<String, String> aggrKeyToAliasMap =new HashMap<String, String>();
		
		try {
			sqlTranslator.addCompositeAggregateNode(node,aggregateNodesMap, aggrKeyToAliasMap, null, table,false);			
		} catch (SqlTranslationException ex) {
			assertTrue(false);
			fail("unexpected SqlTranslationException");
		}
		
	}
	
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testAddCompositeAggregateNodeOperatorNode() throws StandardException{
		String sql = "select count(testdim) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		BinaryArithmeticOperatorNode node =Mockito.mock(BinaryArithmeticOperatorNode.class);

		NumericConstantNode nNode=new NumericConstantNode();
		nNode.init("test",0);
		nNode.setValue(1);
		
		NumericConstantNode rNode=new NumericConstantNode();
		rNode.init("test",0);
		rNode.setValue(1);
		Mockito.when(node.getLeftOperand()).thenReturn(nNode);
		Mockito.when(node.getRightOperand()).thenReturn(rNode);

		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		
		Map<String, AggregateNode> aggregateNodesMap =new HashMap<String, AggregateNode>();
		aggregateNodesMap.put("count", new AggregateNode());

		Map<String, String> aggrKeyToAliasMap =new HashMap<String, String>();
		try {
			sqlTranslator.addCompositeAggregateNode(node,aggregateNodesMap, aggrKeyToAliasMap, null, table,false);			
		} catch (SqlTranslationException ex) {
			assertTrue(false);
			fail("unexpected SqlTranslationException");
		}

		
	}
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testAddCompositeAggregateNodeOperatorNodeLeft() throws StandardException{
		String sql = "select count(testdim) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		BinaryArithmeticOperatorNode node =Mockito.mock(BinaryArithmeticOperatorNode.class);

		ColumnReference nNode= Mockito.mock(ColumnReference.class);
		Mockito.when(nNode.getColumnName()).thenReturn("testdim");
		
		NumericConstantNode rNode=new NumericConstantNode();
		rNode.init("test",0);
		rNode.setValue(1);
		Mockito.when(node.getLeftOperand()).thenReturn(nNode);
		Mockito.when(node.getRightOperand()).thenReturn(nNode);

		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testDim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		
		Map<String, AggregateNode> aggregateNodesMap =new HashMap<String, AggregateNode>();
		aggregateNodesMap.put("count", new AggregateNode());

		Map<String, String> aggrKeyToAliasMap =new HashMap<String, String>();
		try {
			sqlTranslator.addCompositeAggregateNode(node,aggregateNodesMap, aggrKeyToAliasMap, null, table,false);			
		} catch (SqlTranslationException ex) {
			assertTrue(false);
			fail("unexpected SqlTranslationException");
		}

		
	}
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testcolumnRefCheck() throws StandardException{
		String sql = "select count(testdim) as testdim from tabletest where (site=0 or not(region='11')) order by notexist";
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				new CallsRealMethods());
		Mockito.doReturn("")
				.when(sqlTranslator)
				.checkNameChange(Mockito.any(ColumnReference.class),
						Mockito.any(Table.class), Mockito.any(Map.class));

		assertEquals("tabletest", sqlTranslator.getTableName(sql));
		sqlTranslator.parse(sql);
		BinaryArithmeticOperatorNode node =Mockito.mock(BinaryArithmeticOperatorNode.class);

		ColumnReference nNode= Mockito.mock(ColumnReference.class);
		Mockito.when(nNode.getColumnName()).thenReturn("testdim");
		
		NumericConstantNode rNode=new NumericConstantNode();
		rNode.init("test",0);
		rNode.setValue(1);
		Mockito.when(node.getLeftOperand()).thenReturn(nNode);
		Mockito.when(node.getRightOperand()).thenReturn(nNode);

		TableDimension dimension = new TableDimension();
		dimension.setName("testdim");
		dimension.setType(0);
		dimension.setMultiValue(true);
		assertEquals("testdim", dimension.getName());
		assertEquals(0, dimension.getType());

		Table table = new Table();
		table.setTableName("tabletest");
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setDimensions(Lists.newArrayList(dimension));
		
		Map<String, AggregateNode> aggregateNodesMap =new HashMap<String, AggregateNode>();
		aggregateNodesMap.put("count", new AggregateNode());

		try {
			sqlTranslator.columnRefCheck(nNode, table, null, true, false);	
			fail("expected SqlTranslationException");
		} catch (SqlTranslationException ex) {
			assertTrue(true);

		}

		
	}


}
