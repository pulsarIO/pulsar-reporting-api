/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.holap.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.ebay.pulsar.analytics.constants.Constants;
import com.ebay.pulsar.analytics.datasource.DBConnector;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.DataSourceProvider;
import com.ebay.pulsar.analytics.datasource.PulsarDataBaseConnector;
import com.ebay.pulsar.analytics.datasource.PulsarRestMetricMeta;
import com.ebay.pulsar.analytics.datasource.PulsarRestMetricRegistry;
import com.ebay.pulsar.analytics.datasource.PulsarTable;
import com.ebay.pulsar.analytics.datasource.PulsarTableDimension;
import com.ebay.pulsar.analytics.datasource.ReflectFieldUtil;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.holap.query.validator.PulsarRestValidator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.CountAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.filter.RegexFilter;
import com.ebay.pulsar.analytics.metricstore.druid.having.EqualToHaving;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.ConstantPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.query.DruidQueryParameter;
import com.ebay.pulsar.analytics.metricstore.druid.query.DruidQueryProcessor;
import com.ebay.pulsar.analytics.metricstore.druid.query.validator.DruidGranularityValidator;
import com.ebay.pulsar.analytics.metricstore.druid.query.validator.GranularityAndTimeRange;
import com.ebay.pulsar.analytics.query.SQLQueryContext;
import com.ebay.pulsar.analytics.query.request.CoreRequest;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.ebay.pulsar.analytics.query.response.TraceAbleResponse;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DataSourceProvider.class,DataSourceMetaRepo.class})
public class RestQueryProcessorImplTest {

	@Test
	public void testRestQueryProcessorImplDruid() throws Exception{

		RestQueryProcessorImpl processor=new RestQueryProcessorImpl();
		
		PulsarDataBaseConnector connector = PowerMockito
				.mock(PulsarDataBaseConnector.class);
		when(
				connector.getOLAPByTableAndIntervals(
						Matchers.anyString(), Matchers.any(DateRange.class)))
				.thenReturn("testDB");
		DataSourceProvider provider = PowerMockito
				.mock(DataSourceProvider.class);
		when(provider.getConnector()).thenReturn(connector);
		PulsarTable table = new PulsarTable();
		table.setTableName("event");
		PulsarTableDimension dimension = new PulsarTableDimension();
		dimension.setName("page");
		dimension.setRTOLAPColumnName("page");
		PulsarTableDimension metric = new PulsarTableDimension();
		metric.setName("pageview");
		table.setDimensions(Lists.newArrayList(dimension));
		table.setMetrics(Lists.newArrayList(metric));
		
		when(provider.getTableByName(Matchers.anyString())).thenReturn(table);
		DataSourceMetaRepo metaRepo = PowerMockito
				.mock(DataSourceMetaRepo.class);
		Mockito.when(metaRepo.getDBMetaFromCache(Matchers.anyString()))
				.thenReturn(provider);
		PowerMockito.mockStatic(DataSourceMetaRepo.class);

		Mockito.when(DataSourceMetaRepo.getInstance()).thenReturn(metaRepo);

		List<PulsarRestMetricMeta> metricsList = new ArrayList<PulsarRestMetricMeta>();
		PulsarRestMetricMeta meta = new PulsarRestMetricMeta();
		meta.setTableName("testtable");
		meta.setMetricName("testmetric");
		meta.setMetricEndpoints(Sets.newHashSet("core"));
		EqualToHaving druidHaving=new EqualToHaving("testdim", "test");
		meta.setDruidHaving(druidHaving);
		metricsList.add(meta);
		PulsarRestMetricRegistry pulsarRestMetricRegistry = new PulsarRestMetricRegistry(
				metricsList);
		PulsarRestValidator pulsarRestValidator = new PulsarRestValidator(
				pulsarRestMetricRegistry);
		CoreRequest coreRequest = new CoreRequest();
		coreRequest.setEndTime("2015-09-15 23:59:59");
		coreRequest.setStartTime("2015-09-09 00:00:00");
		coreRequest.setDimensions(Lists.newArrayList("page"));
		coreRequest.setFilter("page like 'a%' limit 300");
		coreRequest.setGranularity("week");
		coreRequest.setHaving("page >0 limit 300");
		coreRequest.setMaxResults(10);
		coreRequest.setMetrics(Lists.newArrayList("testmetric"));
		coreRequest.setNamespace(Constants.RequestNameSpace.core);
		coreRequest.setSort("page");


		DruidGranularityValidator druidValidator=PowerMockito
				.mock(DruidGranularityValidator.class);
		druidValidator.validate(Matchers.any(GranularityAndTimeRange.class));
		DruidFilterParser druidFilterParser=PowerMockito
				.mock(DruidFilterParser.class);
		RegexFilter filter=new RegexFilter("like", "%a");

		CountAggregator aggregator=new CountAggregator("page");
		List<BaseAggregator> druidAggregators=new ArrayList<BaseAggregator>();
		druidAggregators.add(aggregator);
		meta.setDruidAggregators(druidAggregators);
		ConstantPostAggregator postAggregator=new ConstantPostAggregator("1", 1);
		List<BasePostAggregator> druidPostAggregators=new ArrayList<BasePostAggregator>();
		druidPostAggregators.add(postAggregator);
		meta.setDruidPostAggregators(druidPostAggregators);
		meta.setDruidFilter(filter);
		metricsList.add(meta);
	
		when(druidFilterParser.parseWhere(Matchers.anyString(), Matchers.any(Table.class))).thenReturn(filter);
		DruidQueryProcessor druidQueryProcessor = PowerMockito
				.mock(DruidQueryProcessor.class);
		TraceAbleResponse response=new TraceAbleResponse();
		Mockito.when(druidQueryProcessor.queryDruid(Matchers.any(DruidQueryParameter.class) , Matchers.any(DBConnector.class))).thenReturn(response);
		Mockito.when(druidQueryProcessor.doSQLQuery(Matchers.any(SQLQueryContext.class))).thenReturn(response);		
		ReflectFieldUtil.setField(RestQueryProcessorImpl.class,processor, "pulsarRestMetricRegistry", pulsarRestMetricRegistry);
		ReflectFieldUtil.setField(RestQueryProcessorImpl.class,processor, "restValidator", pulsarRestValidator);
		ReflectFieldUtil.setField(RestQueryProcessorImpl.class,processor, "druidValidator", druidValidator);
		ReflectFieldUtil.setField(RestQueryProcessorImpl.class,processor, "druidFilterParser", druidFilterParser);
		ReflectFieldUtil.setField(RestQueryProcessorImpl.class,processor, "druidQueryProcessor", druidQueryProcessor);

				
		assertEquals(response,processor.executeRestQuery(coreRequest));
		SQLQueryContext context=new SQLQueryContext();
		context.setDbNameSpaces(Lists.newArrayList("test"));
		context.setGranularity("all");
		DateTime startTime=DateTime.parse("2015-09-9T23:59:59");
		DateTime endTime=DateTime.parse("2015-09-15T23:59:59");
		DateRange dataRange = new DateRange(startTime, endTime);
		context.setIntervals(dataRange);
		context.setNs(Constants.RequestNameSpace.sql);
		context.setSqlQuery("");
		context.setTableNames(Lists.newArrayList("event"));
		assertEquals(response,processor.doSQLQuery(context));
		
		when(druidFilterParser.parseWhere(Matchers.anyString(), Matchers.any(Table.class))).thenReturn(null);
		try{
			processor.executeRestQuery(coreRequest);
		}catch(SqlTranslationException ex){
			assertTrue(true);
		}
		
		when(druidFilterParser.parseWhere(Matchers.anyString(), Matchers.any(Table.class))).thenReturn(filter);
		meta.setDruidAggregators(null);
		try{
			processor.executeRestQuery(coreRequest);
		}catch(SqlTranslationException ex){
			assertTrue(true);
		}
		
		meta.setDruidAggregators(druidAggregators);
		coreRequest.setSort(null);
		try{
			processor.executeRestQuery(coreRequest);
		}catch(InvalidQueryParameterException ex){
			assertTrue(true);
		}


	}

}
