/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.LongSumAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.QueryType;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.SortDirection;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.SelectorFilter;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.DurationGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.PeriodGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.having.EqualToHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.GreaterThanHaving;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.DefaultLimitSpec;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.OrderByColumnSpec;
import com.ebay.pulsar.analytics.metricstore.druid.metric.BaseMetric;
import com.ebay.pulsar.analytics.metricstore.druid.metric.NumericMetric;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.ConstantPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.DruidSpecs;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.GroupByQuery;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.TimeSeriesQuery;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.TopNQuery;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class QueryTest {


	@Test
	public void test() {
		testDruidSpecs();
		testGroupByQuery();
		testTimeSeriesQuery();
		testTopNQuery();
	}

	private void testDruidSpecs(){
		String fromTable="pulsar_session";
		List<String> dimensions=Lists.newArrayList("D1","D2");
		Map<String,String> nameAliasMap=ImmutableMap.of("D1", "AD1", "D2", "AD2");
		List<BaseAggregator> aggregators=getAggregators();
		List<BasePostAggregator> postAggregators=getPostAggregators();
		DruidSpecs specs=new DruidSpecs(fromTable,dimensions,nameAliasMap,aggregators,postAggregators);
		
		assertEquals(fromTable,specs.getFromTable());
		assertEquals(dimensions,specs.getDimensions());
		assertEquals(nameAliasMap,specs.getNameAliasMap());
		assertEquals(aggregators,specs.getAggregators());
		assertEquals(postAggregators,specs.getPostAggregators());
		
		BaseFilter filter=getFilter();
		//List<String> intervals=getIntervals();
		specs.setFilter(filter);
		DateTime startTime=DateTime.parse("2015-09-9T23:59:59");
		DateTime endTime=DateTime.parse("2015-09-15T23:59:59");
		DateRange tr = new DateRange(startTime, endTime);
		specs.setIntervals(tr);
		assertEquals(filter,specs.getFilter());
		assertEquals(tr,specs.getIntervals());
		
		specs.setGranularity(BaseGranularity.ALL);
		assertEquals(BaseGranularity.ALL,specs.getGranularity());		
		specs.setLimit(10);
		assertEquals(10,specs.getLimit());
		
		specs.setOffset(20);
		assertEquals(20,specs.getOffset());
		specs.setSort("abc");
		assertEquals("abc",specs.getSort());
		
		String d1="d1";
		OrderByColumnSpec c1=new OrderByColumnSpec(d1,SortDirection.descending);
		DefaultLimitSpec dls=new DefaultLimitSpec(10,Lists.newArrayList(c1));
		specs.setLimitSpec(dls);
		assertEquals(dls,specs.getLimitSpec());
		assertEquals(d1,specs.getSort());
		Table t=new Table();
		specs.setTableColumnsMeta(t);
		assertEquals(t,specs.getTableColumnsMeta());
		
		String aggregate = "Aggregate";
		String value = "123";
		GreaterThanHaving having = new GreaterThanHaving (aggregate, value);
		specs.setHaving(having);
		assertEquals(having,specs.getHaving());
		

		
		assertTrue(specs.equals(specs));
		assertTrue(!specs.equals(null));
		assertTrue(!specs.equals(new Object()));
		
		DruidSpecs specs2=new DruidSpecs(null,null,null,null,null);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		specs2=new DruidSpecs(null,null,null,aggregators,null);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		specs2=new DruidSpecs(null,dimensions,null,aggregators,null);
		specs2.setFilter(null);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		
		specs2=new DruidSpecs(null,dimensions,null,aggregators,null);
		specs2.setFilter(filter);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		
		specs2=new DruidSpecs(fromTable,dimensions,null,aggregators,null);
		specs2.setFilter(filter);
		specs2.setGranularity(null);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));

		specs2.setGranularity(BaseGranularity.ALL);
		specs2.setHaving(null);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		
		specs2.setHaving(having);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		
		specs2.setIntervals(tr);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		
		specs2.setLimit(10);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		
		specs2.setLimitSpec(dls);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		
		specs2.setOffset(20);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		specs2.setSort("abc");
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		specs2.setTableColumnsMeta(t);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		
		specs2=new DruidSpecs(fromTable,dimensions,null,aggregators,postAggregators);
		specs2.setFilter(filter);
		specs2.setIntervals(tr);
		specs2.setGranularity(BaseGranularity.ALL);
		specs2.setLimit(10);
		specs2.setOffset(20);
		specs2.setSort("abc");
		specs2.setLimitSpec(dls);
		specs2.setTableColumnsMeta(t);
		specs2.setHaving(having);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
		
		specs2=new DruidSpecs(fromTable,dimensions,nameAliasMap,aggregators,null);
		specs2.setFilter(filter);
		specs2.setIntervals(tr);
		specs2.setGranularity(BaseGranularity.ALL);
		specs2.setLimit(10);
		specs2.setOffset(20);
		specs2.setSort("abc");
		specs2.setLimitSpec(dls);
		specs2.setTableColumnsMeta(t);
		specs2.setHaving(having);
		assertTrue(!specs2.equals(specs));assertTrue(!specs.equals(specs2));
	
		 specs2=new DruidSpecs(fromTable,dimensions,nameAliasMap,aggregators,postAggregators);
		 specs2.setFilter(filter);
			specs2.setIntervals(tr);
			specs2.setGranularity(BaseGranularity.ALL);
			specs2.setLimit(10);
			specs2.setOffset(20);
			specs2.setSort("abc");
			specs2.setLimitSpec(dls);
			specs2.setTableColumnsMeta(t);
			specs2.setHaving(having);
			assertTrue(specs2.equals(specs));

		assertTrue(specs.hashCode()==specs2.hashCode());
			
	}
	
	public void testGroupByQuery() {
		String dataSource = "GroupByQueryTest";
		List<String> intervals = getIntervals();
		List<String> dimensions = getDimensions();
		List<BaseAggregator> aggregations = getAggregators();
		BaseGranularity granularity = BaseGranularity.ALL;
		int limit = 10;

		// GroupByQuery with SimpleGranularity
		GroupByQuery groupByQuery = new GroupByQuery (dataSource, intervals, granularity, aggregations, dimensions);

		String sort = "metric";
		OrderByColumnSpec orderByColumnSpec = new OrderByColumnSpec(sort, SortDirection.descending);

		String sortGot = orderByColumnSpec.getDimension();
		SortDirection sortDirection = orderByColumnSpec.getDirection();
		assertEquals("Sort NOT Equals", sort, sortGot);
		assertEquals("SortDirection NOT Equals", SortDirection.descending, sortDirection);

		List<OrderByColumnSpec> columns = new ArrayList<OrderByColumnSpec>();
		columns.add(orderByColumnSpec);
		DefaultLimitSpec defaultLimitSpec = new DefaultLimitSpec(limit, columns);

		groupByQuery.setLimitSpec(defaultLimitSpec);

		defaultLimitSpec.getColumns();
		OrderByColumnSpec columnSpecGot = columns.get(0);
		int limitGot = defaultLimitSpec.getLimit();
		String typeLimtSpec = defaultLimitSpec.getType();
		assertEquals("ColumnSpec NOT Equals", sort, columnSpecGot.getDimension());
		assertEquals("Limit NOT Equals", limit, limitGot);
		assertEquals("TYPE NOT Equals", "default", typeLimtSpec);

		String dataSourceGot = groupByQuery.getDataSource();
		List<String> intervalsGot = groupByQuery.getIntervals();
		List<String> dimensionsGot = groupByQuery.getDimensions();
		groupByQuery.getAggregations();


		assertEquals("DataSource NOT Equals", dataSource, dataSourceGot);
		assertEquals("Intervals NOT Equals", intervals.get(0), intervalsGot.get(0));
		assertEquals("Dimensions NOT Equals", dimensions.get(0), dimensionsGot.get(0));

		byte[] cacheKey = groupByQuery.cacheKey();

		String hashCacheKeyExpected = "9f39d23b76cd0fec69ff694978ca116bec16702d";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		groupByQuery.toString();
		BaseFilter filter = getFilter();
		groupByQuery.setFilter(filter);

		String dim = "Aggregate";
		String val = "Value";
		EqualToHaving having = new EqualToHaving (dim, val);

		groupByQuery.setHaving(having);

		List<BasePostAggregator> postAggregations = getPostAggregators ();
		groupByQuery.setPostAggregations(postAggregations);

		cacheKey = groupByQuery.cacheKey();

		hashCacheKeyExpected = "01239ed6b654c8413a5e2459e136f50da89bbb27";
		hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		// GroupByQuery with DurationGranularity
		DurationGranularity durationGranularity1 = new DurationGranularity("7200000");
		DurationGranularity durationGranularity2 = new DurationGranularity("7200000","1970-01-01T00:07:00Z");

		GroupByQuery groupByQuery1 = new GroupByQuery (dataSource, intervals, durationGranularity1, aggregations, dimensions);
		GroupByQuery groupByQuery2 = new GroupByQuery (dataSource, intervals, durationGranularity2, aggregations, dimensions);

		groupByQuery1.setLimitSpec(defaultLimitSpec);
		groupByQuery2.setLimitSpec(defaultLimitSpec);

		byte[] cacheKey1 = groupByQuery1.cacheKey();

		String hashCacheKeyExpected1 = "e92e05aa14c9e21bbdbb64268ef5ecf60887e291";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);

		byte[] cacheKey2 = groupByQuery2.cacheKey();

		String hashCacheKeyExpected2 = "fd1c68cf2b3c95f7845d5d85255010d4627a66a5";
		String hashCacheKeyGot2 = DigestUtils.shaHex(cacheKey2);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected2, hashCacheKeyGot2);

		durationGranularity1.getOrigin();
		durationGranularity1.getDuration();

		PeriodGranularity periodGranularity1 = new PeriodGranularity ("P2D");
		PeriodGranularity periodGranularity2 = new PeriodGranularity ("P2D", "MST");
		PeriodGranularity periodGranularity3 = new PeriodGranularity ("P2D", "MST", "1970-01-01T00:07:00");

		GroupByQuery groupByQueryP1 = new GroupByQuery (dataSource, intervals, periodGranularity1, aggregations, dimensions);
		GroupByQuery groupByQueryP2 = new GroupByQuery (dataSource, intervals, periodGranularity2, aggregations, dimensions);
		GroupByQuery groupByQueryP3 = new GroupByQuery (dataSource, intervals, periodGranularity3, aggregations, dimensions);

		periodGranularity1.getOrigin();
		periodGranularity1.getTimeZone();
		periodGranularity1.getPeriod();

		groupByQueryP1.setLimitSpec(defaultLimitSpec);
		groupByQueryP2.setLimitSpec(defaultLimitSpec);
		groupByQueryP3.setLimitSpec(defaultLimitSpec);

		byte[] cacheKeyP1 = groupByQueryP1.cacheKey();

		String hashCacheKeyExpectedP1 = "45b384d4efbc33dba3324d7ea34ea12c6b6c0829";
		String hashCacheKeyGotP1 = DigestUtils.shaHex(cacheKeyP1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedP1, hashCacheKeyGotP1);

		byte[] cacheKeyP2 = groupByQueryP2.cacheKey();

		String hashCacheKeyExpectedP2 = "965b913a29ea5ddc646319219f0bbfcb39ba7827";
		String hashCacheKeyGotP2 = DigestUtils.shaHex(cacheKeyP2);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedP2, hashCacheKeyGotP2);

		byte[] cacheKeyP3 = groupByQueryP3.cacheKey();

		String hashCacheKeyExpectedP3 = "4d1028224ec58076f47f7ec1d9cac5e2408a624a";
		String hashCacheKeyGotP3 = DigestUtils.shaHex(cacheKeyP3);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedP3, hashCacheKeyGotP3);

		QueryType type = groupByQuery.getQueryType();
		assertEquals("Type NOT Equals", QueryType.groupBy, type);
		Constants constants = new Constants ();
		assertNotNull(constants);
	}

	public void testTimeSeriesQuery() {
		String dataSource = "TimeSeriesQueryTest";
		List<String> intervals = getIntervals();
		List<BaseAggregator> aggregations = getAggregators();
		BaseGranularity granularity = BaseGranularity.ALL;

		TimeSeriesQuery timeSeriesQuery = new TimeSeriesQuery (dataSource, intervals, granularity, aggregations);

		String dataSourceGot = timeSeriesQuery.getDataSource();
		List<String> intervalsGot = timeSeriesQuery.getIntervals();
		timeSeriesQuery.getAggregations();
		BaseGranularity granularityGot = timeSeriesQuery.getGranularity();
		assertEquals("DataSource NOT Equals", dataSource, dataSourceGot);
		assertEquals("Intervals NOT Equals", intervals.get(0), intervalsGot.get(0));
		assertEquals("Granularity NOT Equals", granularity, granularityGot);

		byte[] cacheKey = timeSeriesQuery.cacheKey();

		String hashCacheKeyExpected = "e550ce5f344c44103d79536edc332aff7fda3756";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		timeSeriesQuery.toString();

		List<BasePostAggregator> postAggregations = getPostAggregators ();
		timeSeriesQuery.setPostAggregations(postAggregations);

		cacheKey = timeSeriesQuery.cacheKey();

		hashCacheKeyExpected = "0c542f5ea795551b325bf6390a32e8dc3a0be805";
		hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		PeriodGranularity periodGranularity1 = new PeriodGranularity ("P2D");
		PeriodGranularity periodGranularity2 = new PeriodGranularity ("P2D", "MST");
		PeriodGranularity periodGranularity3 = new PeriodGranularity ("P2D", "MST", "1970-01-01T00:07:00");

		TimeSeriesQuery timeSeriesQueryP1 = new TimeSeriesQuery (dataSource, intervals, periodGranularity1, aggregations);
		TimeSeriesQuery timeSeriesQueryP2 = new TimeSeriesQuery (dataSource, intervals, periodGranularity2, aggregations);
		TimeSeriesQuery timeSeriesQueryP3 = new TimeSeriesQuery (dataSource, intervals, periodGranularity3, aggregations);

		byte[] cacheKey1 = timeSeriesQueryP1.cacheKey();

		String hashCacheKeyExpected1 = "ac122b80f8600d94b7ebbf8917bf1c8d70224a12";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);

		byte[] cacheKey2 = timeSeriesQueryP2.cacheKey();

		String hashCacheKeyExpected2 = "6cc28ac5f7feffa54e2ff949d3d7bf0696c0dd0b";
		String hashCacheKeyGot2 = DigestUtils.shaHex(cacheKey2);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected2, hashCacheKeyGot2);

		byte[] cacheKey3 = timeSeriesQueryP3.cacheKey();

		String hashCacheKeyExpected3 = "e42b59020c77cadb62b75a31c14b62da8aa899dd";
		String hashCacheKeyGot3 = DigestUtils.shaHex(cacheKey3);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected3, hashCacheKeyGot3);

		QueryType type = timeSeriesQuery.getQueryType();
		assertEquals("Type NOT Equals", QueryType.timeseries, type);
	}

	public void testTopNQuery() {
		String dataSource = "TopNQueryTest";
		String dimension = "Dimensions1";
		List<String> intervals = getIntervals();
		List<BaseAggregator> aggregations = getAggregators();
		BaseGranularity granularity = BaseGranularity.ALL;
		int threshold = 10;
		BaseMetric metric = getMetric();

		TopNQuery topNQuery = new TopNQuery (dataSource, intervals, granularity, aggregations, dimension, threshold, metric);

		String dataSourceGot = topNQuery.getDataSource();
		String dimensionGot = topNQuery.getDimension();
		List<String> intervalsGot = topNQuery.getIntervals();
		topNQuery.getAggregations();
		BaseGranularity granularityGot = topNQuery.getGranularity();
		assertEquals("DataSource NOT Equals", dataSource, dataSourceGot);
		assertEquals("Intervals NOT Equals", intervals.get(0), intervalsGot.get(0));
		assertEquals("Dimensions NOT Equals", dimension, dimensionGot);
		assertEquals("Granularity NOT Equals", granularity, granularityGot);

		byte[] cacheKey = topNQuery.cacheKey();

		String hashCacheKeyExpected = "0492640363784ebece072be41b958ae170710b80";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		topNQuery.toString();

		// TopNQuery with DurationGranularity
		DurationGranularity granularityDuration = new DurationGranularity("7200000");

		TopNQuery topNQuery1 = new TopNQuery (dataSource, intervals, granularityDuration, aggregations, dimension, threshold, metric);

		byte[] cacheKey1 = topNQuery1.cacheKey();

		String hashCacheKeyExpected1 = "6694ce551af03bef2b655a55f73e978cec53c05c";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);

		List<BasePostAggregator> postAggregations = getPostAggregators ();
		topNQuery1.setPostAggregations(postAggregations);

		cacheKey1 = topNQuery1.cacheKey();

		hashCacheKeyExpected1 = "303a95fb1ad9991c77b8efffab88d0ecb8628350";
		hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);

		QueryType type = topNQuery.getQueryType();
		assertEquals("Type NOT Equals", QueryType.topN, type);
	}

	List<String> getIntervals () {
		List<String> intervals = new ArrayList<String> ();
		intervals.add("2015-06-18 01:23:52");
		intervals.add("2015-06-19 01:23:52");
		return intervals;
	}

	List<String> getDimensions () {
		List<String> dimensions = new ArrayList<String> ();
		// Two or more
		dimensions.add("Dimension1");
		dimensions.add("Dimension2");
		return dimensions;
	}

	List<BaseAggregator> getAggregators () {
		String aggregatorName = "LongSumAggrTest";
		String fieldName = "FieldName";

		LongSumAggregator longSumAggr = new LongSumAggregator (aggregatorName, fieldName);

		List<BaseAggregator> aggregations = new ArrayList<BaseAggregator> ();

		aggregations.add(longSumAggr);
		return aggregations;
	}

	BaseMetric getMetric () {
		String metricName = "NumericMetric";
		NumericMetric numericMetric = new NumericMetric (metricName);
		return numericMetric;
	}

	BaseFilter getFilter () {
		String dim = "Dimension";
		String val = "Value";

		SelectorFilter filter = new SelectorFilter (dim, val);
		return filter;
	}

	List<BasePostAggregator> getPostAggregators () {
		List<BasePostAggregator> postAggregations = new ArrayList<BasePostAggregator> ();
		String postAggrName = "ConstantPostAggrTest";
		Long valueLong = 1001L;

		ConstantPostAggregator constantPostAggr = new ConstantPostAggregator (postAggrName, valueLong);
		postAggregations.add(constantPostAggr);
		return postAggregations;
	}
}
