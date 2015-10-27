/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.ClientBuilder;

import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.ebay.pulsar.analytics.constants.Constants.RequestNameSpace;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.LongSumAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.SortDirection;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.SelectorFilter;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.having.GreaterThanHaving;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.DefaultLimitSpec;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.OrderByColumnSpec;
import com.ebay.pulsar.analytics.metricstore.druid.metric.BaseMetric;
import com.ebay.pulsar.analytics.metricstore.druid.metric.NumericMetric;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.ConstantPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.query.DruidQueryParameter;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.DruidSpecs;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

@RunWith(PowerMockRunner.class)  //1
@PrepareForTest({ClientBuilder.class})
public class DruidQueryParameterTest {

	@Test
	public void test() throws Exception {
		testDruidQueryParameter();
	}
	private void testDruidQueryParameter(){
		
		String fromTable="pulsar_session";
		List<String> dimensions=Lists.newArrayList("D1","D2");
		Map<String,String> nameAliasMap=ImmutableMap.of("D1", "AD1", "D2", "AD2");
		List<BaseAggregator> aggregators=getAggregators();
		List<BasePostAggregator> postAggregators=getPostAggregators();	
		BaseFilter filter=getFilter();
		getIntervals();
		DateTime startTime=DateTime.parse("2015-09-9T23:59:59");
		DateTime endTime=DateTime.parse("2015-09-15T23:59:59");
		DateRange tr = new DateRange(startTime, endTime);
		String d1="d1";
		OrderByColumnSpec c1=new OrderByColumnSpec(d1,SortDirection.descending);
		DefaultLimitSpec dls=new DefaultLimitSpec(10,Lists.newArrayList(c1));
		Table t=new Table();
		String aggregate = "Aggregate";
		String value = "123";
		GreaterThanHaving having = new GreaterThanHaving (aggregate, value);

		List<String> dbNameSpaces=Lists.newArrayList("druid");
	
		DruidSpecs specs = new DruidSpecs(fromTable, dimensions, nameAliasMap,
				aggregators, postAggregators);
		specs.setFilter(filter);
		specs.setIntervals(tr);
		specs.setGranularity(BaseGranularity.ALL);
		specs.setLimit(10);
		specs.setOffset(20);
		specs.setSort("abc");
		specs.setLimitSpec(dls);
		specs.setTableColumnsMeta(t);
		specs.setHaving(having);

		DruidQueryParameter parameter=new DruidQueryParameter(specs,RequestNameSpace.core,dbNameSpaces);
		assertTrue(parameter.equals(parameter));
		assertTrue(!parameter.equals(null));
		assertTrue(!parameter.equals(new Object()));
		
		DruidQueryParameter parameter2=new DruidQueryParameter(specs,RequestNameSpace.core);
		assertTrue(!parameter.equals(parameter2));assertTrue(!parameter2.equals(parameter));
		
		parameter2=new DruidQueryParameter(null,RequestNameSpace.core,dbNameSpaces);
		assertTrue(!parameter.equals(parameter2));assertTrue(!parameter2.equals(parameter));
		
		parameter2 =new DruidQueryParameter(specs,null,dbNameSpaces);
		assertTrue(!parameter.equals(parameter2));assertTrue(!parameter2.equals(parameter));

		parameter2 =new DruidQueryParameter(specs,RequestNameSpace.core,dbNameSpaces);
		assertTrue(parameter.equals(parameter2));
		assertEquals(specs,parameter.getDruidSpecs());
		assertEquals(RequestNameSpace.core,parameter.getNs());
		assertEquals(dbNameSpaces,parameter.getDbNameSpaces());
		assertEquals(parameter.hashCode(), parameter2.hashCode());
		
		//UT to coverage useless code.
		parameter2.setDbNameSpaces(null);
		parameter2.setDruidSpecs(null);
		parameter2.setNs(null);
		assertNull(parameter2.getDbNameSpaces());
		assertNull(parameter2.getDruidSpecs());
		assertNull(parameter2.getNs());
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
