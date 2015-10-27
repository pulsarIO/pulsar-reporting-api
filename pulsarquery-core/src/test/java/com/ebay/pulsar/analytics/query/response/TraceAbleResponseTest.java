/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.response;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class TraceAbleResponseTest {
	@Test
	public void testTraceQuery() {
		TraceQuery traceQuery=new TraceQuery();
		traceQuery.setBytesize(10);
		traceQuery.setCachegettime(1100101023440L);
		traceQuery.setCachekey("test".getBytes());
		traceQuery.setDruidquerytime(20150918L);
		traceQuery.setFromcache(false);
		traceQuery.setTocache(false);
		traceQuery.setQuery("select * from druid");
		assertEquals(10,traceQuery.getBytesize());
		assertEquals(1100101023440L,traceQuery.getCachegettime());
		assertEquals("test".getBytes().length,traceQuery.getCachekey().length);
		assertEquals(20150918L,traceQuery.getDruidquerytime());
		assertEquals(false,traceQuery.isFromcache());
		assertEquals(false,traceQuery.isTocache());
		assertEquals("select * from druid",traceQuery.getQuery());
	}
	
	@Test
	public void testTraceAbleResponse(){
		TraceAbleResponse response=new TraceAbleResponse();
		TraceQuery traceQuery=new TraceQuery();
		traceQuery.setBytesize(10);
		traceQuery.setCachegettime(1100101023440L);
		traceQuery.setCachekey("test".getBytes());
		traceQuery.setDruidquerytime(20150918L);
		traceQuery.setFromcache(false);
		traceQuery.setTocache(false);
		traceQuery.setQuery("select * from druid");
		response.setQuery(traceQuery);
		response.setRequestProcessTime(10000000L);
		assertEquals(traceQuery,response.getQuery());
		assertEquals(10000000L,response.getRequestProcessTime());
		
		List<Map<String,Object>> results=new ArrayList<Map<String,Object>>();
		Map<String,Object> result=new HashMap<String,Object>();
		result.put("result", "test");
		results.add(result);
		response.setQueryResult(results);
		assertEquals(results,response.getQueryResult());
		
	}
}
