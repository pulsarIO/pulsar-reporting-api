/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.ebay.pulsar.analytics.ReflectFieldUtil;
import com.ebay.pulsar.analytics.datasource.ClientHelper;
import com.ebay.pulsar.analytics.datasource.DBConnector;
import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.DataSourceTypeEnum;
import com.ebay.pulsar.analytics.datasource.DruidDataSourceProviderFactory;
import com.ebay.pulsar.analytics.datasource.DruidRestDBConnector;
import com.ebay.pulsar.analytics.datasource.DruidRestTableMeta;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.DataSourceException;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.LongSumAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.SelectorFilter;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.metric.BaseMetric;
import com.ebay.pulsar.analytics.metricstore.druid.metric.NumericMetric;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.ConstantPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.GroupByQuery;
import com.ebay.pulsar.analytics.util.JsonUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@RunWith(PowerMockRunner.class)  //1
@PrepareForTest({ResteasyClientBuilder.class,ClientHelper.class})
public class DataSourceTest {

	@Before
	public void setup() throws Exception {
	}

	@After
	public void after() throws Exception {
	}

	@Test
	public void test() throws Exception {
		testDruidRestTableMeta();
		testDataSourceProviderFactory();
		testDruidRestDBConnector();
	}
	private void testDruidRestTableMeta(){
		Set<String> dims=Sets.newHashSet("D1","D2");
		Set<String> ms=Sets.newHashSet("M1","M2");
		DruidRestTableMeta m1=new DruidRestTableMeta();
		m1.setDimensions(dims);
		m1.setMetrics(ms);
		
		assertEquals(m1,m1);
		assertEquals(dims,m1.getDimensions());
		assertEquals(ms,m1.getMetrics());
		
		DruidRestTableMeta m2=new DruidRestTableMeta();
		assertTrue(!m1.equals(m2));
		m2=new DruidRestTableMeta();
		m2.setDimensions(dims);
		assertTrue(!m1.equals(m2));
		m2.setDimensions(null);
		m2.setMetrics(ms);
		assertTrue(!m1.equals(m2));assertTrue(!m2.equals(m1));
		
		m2.setDimensions(dims);
		m2.setMetrics(ms);
		assertTrue(m1.equals(m2));assertTrue(m2.equals(m1));
		
		
		Set<String> dims2=Sets.newHashSet("D21","D22");
		Set<String> ms2=Sets.newHashSet("M21","M22");
		m2.setDimensions(dims2);
		m2.setMetrics(ms);
		assertTrue(!m1.equals(m2));
		m2.setDimensions(null);
		m2.setMetrics(ms);
		assertTrue(!m1.equals(m2));assertTrue(!m2.equals(m1));
		
		m2.setDimensions(dims);
		m2.setMetrics(ms2);
		assertTrue(!m1.equals(m2));assertTrue(!m2.equals(m1));
		m2.setMetrics(null);
		assertTrue(!m1.equals(m2));assertTrue(!m2.equals(m1));
		
		m2.setDimensions(dims);
		m2.setMetrics(ms);
		assertTrue(!m1.equals(null));
		assertTrue(m1.hashCode()==m2.hashCode());
		assertTrue(!m1.equals(new Object(){}));
		
	}
	private void testDataSourceProviderFactory() throws Exception{
		DBConnector connector=Mockito.mock(DBConnector.class);
		DruidDataSourceProviderFactory providerFactory=PowerMockito.mock(DruidDataSourceProviderFactory.class);
		when(providerFactory.getDBCollector(Mockito.any(DataSourceConfiguration.class))).thenReturn(connector);
		ReflectFieldUtil.setStaticFinalField(DruidDataSourceProviderFactory.class, "instance", providerFactory);
		assertEquals(providerFactory,DruidDataSourceProviderFactory.getInstance());
		assertEquals(connector,DruidDataSourceProviderFactory.getInstance().getDBCollector(new DataSourceConfiguration(DataSourceTypeEnum.DRUID,"abc")));
		
	}
	
	@SuppressWarnings("rawtypes")
	private void testDruidRestDBConnector() throws Exception{
		String endpoint="http://test";
		Client client=Mockito.mock(ResteasyClient.class);
		//PowerMockito..mockStatic(ClientBuilder.class);
		//when(ClientBuilder.newClient(Mockito.any(Configuration.class))).thenReturn(client);
		ResteasyClientBuilder easybuilder=Mockito.mock(ResteasyClientBuilder.class);
		PowerMockito.whenNew(ResteasyClientBuilder.class).withNoArguments().thenReturn(easybuilder);
		when(easybuilder.connectionPoolSize(20)).thenReturn(easybuilder);
		when(easybuilder.connectionTTL(2, TimeUnit.SECONDS)).thenReturn(easybuilder);
		when(easybuilder.establishConnectionTimeout(10,TimeUnit.SECONDS)).thenReturn(easybuilder);
		when(easybuilder.socketTimeout(60,TimeUnit.SECONDS)).thenReturn(easybuilder);
		when(easybuilder.build()).thenReturn((ResteasyClient)client);
		
		WebTarget target=Mockito.mock(ResteasyWebTarget.class);
		when(client.target(Mockito.eq(endpoint))).thenReturn(target);

		
		when(target.path(Mockito.eq("/"))).thenReturn(target);
		when(target.queryParam("pretty")).thenReturn(target);
		
		when(target.path(Mockito.eq("datasources"))).thenReturn(target);
		Builder builder = Mockito.mock(Builder.class);
		when(target.request(Mockito.eq(MediaType.APPLICATION_JSON_TYPE))).thenReturn(builder);
		//Builder builder = dataSourceRs.request(MediaType.APPLICATION_JSON_TYPE);
		//builder.accept(MediaType.APPLICATION_JSON);
		Response response=Mockito.mock(Response.class);
		when(builder.get()).thenReturn(response);
		when(builder.post(Mockito.any(Entity.class))).thenReturn(response);
		when(response.readEntity(Mockito.eq(Map.class))).thenReturn(Maps.newHashMap());
		when(response.getStatus()).thenReturn(Status.OK.getStatusCode());
		String content="[\"pulsar_event_items\",\"pulsar_seo_attri\",\"pulsar_ogmb\",\"pulsar_ogmb_h\",\"pulsar_session_h\",\"pulsar_session\",\"pulsar_event\",\"epsession\",\"pulsar_ogmb_m\",\"pulsar_event_h\"]";
		Set<?> result=JsonUtil.readValue(content, Set.class); 
		when(response.readEntity(Mockito.eq(Set.class))).thenReturn(result);
		List typeResult=new ArrayList();
		when(response.readEntity(Mockito.eq(List.class))).thenReturn(typeResult);	
		
		DataSourceConfiguration config=new DataSourceConfiguration(DataSourceTypeEnum.DRUID,"UTTEST");
		config.setEndPoint(Lists.newArrayList(endpoint));
		
		DruidRestDBConnector connector=new DruidRestDBConnector(config);
		Set<String> tables=connector.getAllTables();
		
		assertTrue(tables==result);
		
		Set<String> tables2=connector.getAllTables(endpoint);
		assertTrue(tables2==result);
		
		when(target.path(Mockito.eq("datasources"))).thenReturn(target);
		when(target.path(Mockito.eq("pulsar_session"))).thenReturn(target);

		String metaContent="{\"dimensions\":[\"uid\",\"region\",\"imgsearch\",\"browserfamily\",\"linespeed\",\"exitpage\",\"sojec\",\"plsec\",\"trafficsource\",\"dmgage\",\"dmgchn\",\"trafficsourceview\",\"osversion\",\"city\",\"clickct\",\"tenant\",\"cobrand\",\"dmgreg\",\"devicefamily\",\"dmgopc\",\"osfamily\",\"site\",\"app\",\"trafficsourceid\",\"entrypage\",\"deviceclass\",\"refdomain\",\"dmginl\",\"refdomainview\",\"country\",\"_bounce\",\"dmggd\",\"browserversion\",\"continent\",\"dmgmrg\",\"pagetype\",\"dmgct\",\"rv\",\"pagect\"],\"metrics\":[\"asqct_ag\",\"vict_ag\",\"count\",\"srpct_ag\",\"totaleventct_ag\",\"totalpagect_ag\",\"hpct_ag\",\"addtocartct_ag\",\"sessionduration_ag\",\"boct_ag\",\"bounce\",\"watchct_ag\",\"bidct_ag\",\"retvisitor\",\"binct_ag\",\"addtolistct_ag\"]}";
		DruidRestTableMeta metaResult=JsonUtil.readValue(metaContent, DruidRestTableMeta.class);
		when(response.readEntity(Mockito.eq(DruidRestTableMeta.class))).thenReturn(metaResult);

		
		
		Table t=connector.getTableMeta("pulsar_session");
		Table t2=connector.getTableMeta("pulsar_session", endpoint);
		assertEquals(t,t2);
		t=connector.getTableMeta(null);
		t2=connector.getTableMeta(null, endpoint);
		assertNull(t);
		assertNull(t2);
		
		
		
		when(target.path(Mockito.eq("/"))).thenReturn(target);
		when(target.queryParam("pretty")).thenReturn(target);
		String stringResult="test string result.";
		when(builder.post(Mockito.any(Entity.class))).thenReturn(response);
		when(response.readEntity(String.class)).thenReturn(stringResult);
		String dataSource = "GroupByQueryTest";
		List<String> intervals = getIntervals();
		List<String> dimensions = getDimensions();
		List<BaseAggregator> aggregations = getAggregators();
		BaseGranularity granularity = BaseGranularity.ALL;
		//int limit = 10;
		// GroupByQuery with SimpleGranularity
		GroupByQuery groupByQuery = new GroupByQuery (dataSource, intervals, granularity, aggregations, dimensions);

		String groupByQueryResult=connector.sendQuery(groupByQuery);
		assertEquals(stringResult,groupByQueryResult);
		String groupByQueryResult2=connector.sendQuery(groupByQuery, endpoint);
		assertEquals(stringResult,groupByQueryResult2);
		
		Object obj=connector.query(groupByQuery);
		assertEquals(stringResult,obj);
		
		when(response.getStatus()).thenReturn(Status.BAD_REQUEST.getStatusCode());

		
		try{
			connector.getAllTables(endpoint);
			fail("Should throw DataSourceException.");
		}catch(DataSourceException e){
			assertTrue(true);
		}
		try{
			t=connector.getTableMeta("pulsar_session");
			fail("Should throw DataSourceException.");
		}catch(DataSourceException e){
			assertTrue(true);
		}
		DataSourceConfiguration config2=new DataSourceConfiguration(DataSourceTypeEnum.DRUID,"UTTEST");
		config2.setEndPoint(Lists.<String>newArrayList(endpoint));
		DruidRestDBConnector connector2=new DruidRestDBConnector(config2);
		try{
			connector2.getAllTables();
			fail("Should throw DataSourceException.");
		}catch(DataSourceException e){
			assertTrue(true);
		}
		try{
			connector2.getTableMeta("pulsar_session");
			fail("Should throw DataSourceException.");
		}catch(DataSourceException e){
			assertTrue(true);
		}
		try{
			connector2.sendQuery(groupByQuery);
			fail("Should throw DataSourceException.");
		}catch(DataSourceException e){
			assertTrue(true);
		}
		try{
			connector2.sendQuery(groupByQuery, endpoint);
			fail("Should throw DataSourceException.");
		}catch(DataSourceException e){
			assertTrue(true);
		}
		
		try{
			connector.sendQuery(groupByQuery);
			fail("Should throw DataSourceException.");
		}catch(DataSourceException e){
			assertTrue(true);
		}
		
		connector.close();
		try{
			//test checkState when closed.
			connector.sendQuery(groupByQuery);
			fail("Should throw DataSourceException.");
		}catch(DataSourceException e){
			assertTrue(true);
		}
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
