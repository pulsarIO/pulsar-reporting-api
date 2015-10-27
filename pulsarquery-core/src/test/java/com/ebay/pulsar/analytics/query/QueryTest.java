/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.Mockito;

import com.ebay.pulsar.analytics.constants.Constants;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.query.client.ClientQueryConfig;
import com.ebay.pulsar.analytics.query.request.CoreRequest;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.ebay.pulsar.analytics.query.request.PulsarDateTimeFormatter;
import com.ebay.pulsar.analytics.query.request.RealtimeRequest;
import com.ebay.pulsar.analytics.query.request.SQLRequest;
import com.ebay.pulsar.analytics.query.sql.SQLTranslator;
import com.google.common.collect.Lists;

public class QueryTest {

	@Test
	public void testGetTableName() {
		String sqlInsert1="select count(clickcount_ag) as \"clickcount_ag\", browserfamily from pulsar_ogmba group by browserfamily limit 100";
		SQLTranslator translator = Mockito.mock(SQLTranslator.class, Mockito.CALLS_REAL_METHODS);
		System.out.println(translator.parse(sqlInsert1));
	}
	
	@Test
	public void testSQLRequest(){
		SQLRequest sqlRequest=new SQLRequest();
		
		sqlRequest.setCustomTime("testCustomTime");
		sqlRequest.setEndTime("testEndTime");
		sqlRequest.setGranularity("day");
		sqlRequest.setIntervals("testInterval");
		sqlRequest.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest.setSql("testSQL");
		sqlRequest.setStartTime("testStartTime");
		
		assertEquals("testCustomTime",sqlRequest.getCustomTime());
		assertEquals("testEndTime",sqlRequest.getEndTime());
		assertEquals("day",sqlRequest.getGranularity());
		assertEquals("testInterval",sqlRequest.getIntervals());
		assertEquals(Constants.RequestNameSpace.sql,sqlRequest.getNamespace());
		assertEquals("testSQL",sqlRequest.getSql());
		assertEquals("testStartTime",sqlRequest.getStartTime());
	}
	
	@Test
	public void testCoreRequest(){
		CoreRequest coreRequest=new CoreRequest();
		DateTime startTime=DateTime.parse("2015-09-15T23:59:59");
		DateTime endTime=DateTime.parse("2015-09-15T23:59:59");
		DateRange dataRange = new DateRange(startTime, endTime);	
		DateRange dataRange2 = new DateRange(startTime, endTime);
		DateRange dataRange3 = new DateRange(null, endTime);
		DateRange dataRange4 = new DateRange(startTime, null);
		assertTrue(dataRange.equals(dataRange2));
		assertTrue(dataRange.hashCode()==dataRange2.hashCode());
		assertFalse(dataRange3.equals(dataRange));
		assertFalse(dataRange4.equals(dataRange));
		
		coreRequest.setEndTime("2015-09-15 23:59:59");
		coreRequest.setStartTime("2015-09-09 00:00:00");
		coreRequest.setDimensions(Lists.newArrayList("testDim"));
		coreRequest.setFilter("testFilter");
		coreRequest.setGranularity("testGranularity");
		coreRequest.setHaving("testHaving");
		coreRequest.setMaxResults(0);
		coreRequest.setMetrics(Lists.newArrayList("testMetric"));
		coreRequest.setNamespace(Constants.RequestNameSpace.core);
		coreRequest.setSort("testSort");
		assertEquals("2015-09-15 23:59:59",coreRequest.getEndTime());
		assertEquals("testGranularity",coreRequest.getGranularity());
		assertEquals("2015-09-15T23:59:59.000-07:00",coreRequest.getQueryDateRange().getEnd().toString());
		assertEquals(Constants.RequestNameSpace.core,coreRequest.getNamespace());
		assertEquals("2015-09-09 00:00:00",coreRequest.getStartTime());
		
		CoreRequest coreRequest2=new CoreRequest();
		coreRequest2.setDimensions(Lists.newArrayList("testDim"));
		coreRequest2.setFilter("testFilter");
		coreRequest2.setGranularity("testGranularity");
		coreRequest2.setHaving("testHaving");
		coreRequest2.setMaxResults(0);
		coreRequest2.setMetrics(Lists.newArrayList("testMetric"));
		coreRequest2.setNamespace(Constants.RequestNameSpace.core);
		coreRequest2.setSort("testSort");
		try{
			coreRequest2.getQueryDateRange();
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		
		CoreRequest coreRequest3=new CoreRequest();
		coreRequest3.setEndTime("2015-09-15 23:59:59");
		coreRequest3.setStartTime("2015-09-19 00:00:00");
		coreRequest3.setDimensions(Lists.newArrayList("testDim"));
		coreRequest3.setFilter("testFilter");
		coreRequest3.setGranularity("testGranularity");
		coreRequest3.setHaving("testHaving");
		coreRequest3.setMaxResults(0);
		coreRequest3.setMetrics(Lists.newArrayList("testMetric"));
		coreRequest3.setNamespace(Constants.RequestNameSpace.core);
		coreRequest3.setSort("testSort");
		try{
			coreRequest3.getQueryDateRange();
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		
		CoreRequest coreRequest4=new CoreRequest();
		coreRequest4.setEndTime("2015-09-15 23:59:59");
		coreRequest4.setStartTime("2016-09-19 00:00:00");
		coreRequest4.setDimensions(Lists.newArrayList("testDim"));
		coreRequest4.setFilter("testFilter");
		coreRequest4.setGranularity("testGranularity");
		coreRequest4.setHaving("testHaving");
		coreRequest4.setMaxResults(0);
		coreRequest4.setMetrics(Lists.newArrayList("testMetric"));
		coreRequest4.setNamespace(Constants.RequestNameSpace.core);
		coreRequest4.setSort("testSort");
		try{
			coreRequest4.getQueryDateRange();
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
	}
	
	@Test
	public void testRealtimeRequest(){
		RealtimeRequest realRequest=new RealtimeRequest();
		List<String> dimensions = new ArrayList<String>();
		dimensions.add("testDim");
		realRequest.setDimensions(dimensions);
		realRequest.setDuration(100);
		realRequest.setGranularity("day");
		realRequest.setFilter("testFilter");
		realRequest.setNamespace(Constants.RequestNameSpace.realtime);
		realRequest.setHaving("testHaving");
		realRequest.setMetrics(dimensions);
		realRequest.setSort("sort");
		realRequest.setMaxResults(10);
		
		assertTrue(realRequest.getQueryDateRange().getEnd().getMillis()<new DateTime(PulsarDateTimeFormatter.MST_TIMEZONE).getMillis());
		
		
	}
	
	@Test
	public void testQueryFormatter(){
		DateTime startTime=DateTime.parse("2015-09-9T23:59:59");
		DateTime endTime=DateTime.parse("2015-09-15T23:59:59");
		DateRange dataRange = new DateRange(startTime, endTime);
		assertTrue(PulsarDateTimeFormatter.buildStringIntervals(dataRange).size()>0);
		assertEquals("2015-09-09T23:59:59.000-07:00",PulsarDateTimeFormatter.parseIntevalsFromString("2015-09-9T23:59:59-07:00/2015-09-15T23:59:59-07:00").getStart().toString());
	}
	
	@Test
	public void testClientQueryConfig(){
		ClientQueryConfig config=new ClientQueryConfig();
		config.setConnectTimeout(1000);
		config.setLimitFactor(100);
		config.setReadTimeout(200);
		config.setThreadPoolsize(20);
		assertEquals(config.getConnectTimeout(),1000);
		assertTrue(config.getLimitFactor()==100);
		assertEquals(config.getReadTimeout(),200);
		assertEquals(config.getThreadPoolsize(),20);
	}

}
