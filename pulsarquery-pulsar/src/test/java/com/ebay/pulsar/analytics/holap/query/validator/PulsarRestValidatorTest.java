/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.holap.query.validator;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Sets;

import org.junit.Test;

import com.ebay.pulsar.analytics.constants.Constants;
import com.ebay.pulsar.analytics.datasource.PulsarRestMetricMeta;
import com.ebay.pulsar.analytics.datasource.PulsarRestMetricRegistry;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.query.request.CoreRequest;
import com.ebay.pulsar.analytics.query.request.RealtimeRequest;
import com.google.common.collect.Lists;

public class PulsarRestValidatorTest {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
    public void testInvalidMetrics(){
		List<PulsarRestMetricMeta> metricsList=new ArrayList<PulsarRestMetricMeta>();
		PulsarRestMetricMeta meta=new PulsarRestMetricMeta();
		meta.setTableName("testtable");
		meta.setMetricName("testmetric");
		meta.setMetricEndpoints(Sets.newHashSet("core"));
		metricsList.add(meta);
		PulsarRestMetricRegistry pulsarRestMetricRegistry=new PulsarRestMetricRegistry(metricsList);
		PulsarRestValidator pulsarRestValidator = new PulsarRestValidator(pulsarRestMetricRegistry);
		CoreRequest coreRequest=new CoreRequest();
		coreRequest.setEndTime("2015-09-15 23:59:59");
		coreRequest.setStartTime("2015-09-09 00:00:00");
		coreRequest.setDimensions(Lists.newArrayList("testDim"));
		coreRequest.setFilter("testFilter");
		coreRequest.setGranularity("testGranularity");
		coreRequest.setHaving("testHaving");
		coreRequest.setMaxResults(0);
		coreRequest.setMetrics(null);
		coreRequest.setNamespace(Constants.RequestNameSpace.core);
		coreRequest.setSort("testSort");
		try{
			pulsarRestValidator.validate(coreRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		coreRequest.setMetrics(new ArrayList());
		try{
			pulsarRestValidator.validate(coreRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		coreRequest.setMetrics(Lists.newArrayList("test"));
		meta.setMetricEndpoints(null);
		try{
			pulsarRestValidator.validate(coreRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		
		coreRequest.setMetrics(Lists.newArrayList("testmetric"));
		meta.setMetricEndpoints(null);
		try{
			pulsarRestValidator.validate(coreRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		meta.setMetricEndpoints(Sets.newHashSet("real"));
		try{
			pulsarRestValidator.validate(coreRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
    }
	@Test
    public void testInvalidRealRequest(){
		List<PulsarRestMetricMeta> metricsList=new ArrayList<PulsarRestMetricMeta>();
		PulsarRestMetricMeta meta=new PulsarRestMetricMeta();
		meta.setTableName("testtable");
		meta.setMetricName("testdim");
		meta.setMetricEndpoints(Sets.newHashSet("core"));
		metricsList.add(meta);
		PulsarRestMetricRegistry pulsarRestMetricRegistry=new PulsarRestMetricRegistry(metricsList);
		PulsarRestValidator pulsarRestValidator = new PulsarRestValidator(pulsarRestMetricRegistry);
		List<String> dimensions = new ArrayList<String>();
		dimensions.add("testdim");
		RealtimeRequest realRequest=new RealtimeRequest();
		realRequest.setDimensions(dimensions);
		realRequest.setDuration(100);
		realRequest.setGranularity("day");
		realRequest.setFilter("testFilter");
		realRequest.setNamespace(Constants.RequestNameSpace.realtime);
		realRequest.setHaving("testHaving");
		realRequest.setMetrics(dimensions);
		realRequest.setSort("sort");
		realRequest.setMaxResults(10);
		try{
			pulsarRestValidator.validate(realRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		meta.setMetricEndpoints(Sets.newHashSet("realtime"));	
		realRequest.setDuration(-100);
		try{
			pulsarRestValidator.validate(realRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		
    }
	@Test
    public void testInvalidMaxResult(){
		List<PulsarRestMetricMeta> metricsList=new ArrayList<PulsarRestMetricMeta>();
		PulsarRestMetricMeta meta=new PulsarRestMetricMeta();
		meta.setTableName("testtable");
		meta.setMetricName("testmetric");
		meta.setMetricEndpoints(Sets.newHashSet("core"));
		metricsList.add(meta);
		PulsarRestMetricRegistry pulsarRestMetricRegistry=new PulsarRestMetricRegistry(metricsList);
		PulsarRestValidator pulsarRestValidator = new PulsarRestValidator(pulsarRestMetricRegistry);
		CoreRequest coreRequest=new CoreRequest();
		coreRequest.setEndTime("2015-09-15 23:59:59");
		coreRequest.setStartTime("2015-09-09 00:00:00");
		coreRequest.setDimensions(Lists.newArrayList("testDim"));
		coreRequest.setFilter("testFilter");
		coreRequest.setGranularity("testGranularity");
		coreRequest.setHaving("testHaving");
		coreRequest.setMaxResults(0);
		coreRequest.setMetrics(Lists.newArrayList("testmetric"));
		coreRequest.setNamespace(Constants.RequestNameSpace.core);
		coreRequest.setSort("testSort");
		try{
			pulsarRestValidator.validate(coreRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		
    }
	
	@Test
    public void testInvalidDataSource(){
		List<PulsarRestMetricMeta> metricsList=new ArrayList<PulsarRestMetricMeta>();
		PulsarRestMetricMeta meta=new PulsarRestMetricMeta();
		meta.setTableName("testtable");
		meta.setMetricName("testmetric");
		meta.setMetricEndpoints(Sets.newHashSet("core"));
		metricsList.add(meta);
		PulsarRestMetricRegistry pulsarRestMetricRegistry=new PulsarRestMetricRegistry(metricsList);
		PulsarRestValidator pulsarRestValidator = new PulsarRestValidator(pulsarRestMetricRegistry);
		CoreRequest coreRequest=new CoreRequest();
		coreRequest.setEndTime("2015-09-15 23:59:59");
		coreRequest.setStartTime("2015-09-09 00:00:00");
		coreRequest.setDimensions(Lists.newArrayList("testDim"));
		coreRequest.setFilter("testFilter");
		coreRequest.setGranularity("testGranularity");
		coreRequest.setHaving("testHaving");
		coreRequest.setMaxResults(10);
		coreRequest.setMetrics(Lists.newArrayList("testmetric"));
		coreRequest.setNamespace(Constants.RequestNameSpace.core);
		coreRequest.setSort("testSort");
		try{
			pulsarRestValidator.validate(coreRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(Exception ex){
			assertTrue(true);
		}
		
    }
	@Test
    public void testNoMeticMeta(){
		List<PulsarRestMetricMeta> metricsList=new ArrayList<PulsarRestMetricMeta>();
		PulsarRestMetricMeta meta=new PulsarRestMetricMeta();
		metricsList.add(meta);
		PulsarRestMetricRegistry pulsarRestMetricRegistry=new PulsarRestMetricRegistry(metricsList);
		PulsarRestValidator pulsarRestValidator = new PulsarRestValidator(pulsarRestMetricRegistry);
		CoreRequest coreRequest=new CoreRequest();
		coreRequest.setEndTime("2015-09-15 23:59:59");
		coreRequest.setStartTime("2015-09-09 00:00:00");
		coreRequest.setDimensions(Lists.newArrayList("testDim"));
		coreRequest.setFilter("testFilter");
		coreRequest.setGranularity("testGranularity");
		coreRequest.setHaving("testHaving");
		coreRequest.setMaxResults(0);
		coreRequest.setMetrics(Lists.newArrayList("testMatic"));
		coreRequest.setNamespace(Constants.RequestNameSpace.core);
		coreRequest.setSort("testSort");
		try{
			pulsarRestValidator.validate(coreRequest);
			fail("Expect InvalidQueryParameterException");
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}
		
    }
}
