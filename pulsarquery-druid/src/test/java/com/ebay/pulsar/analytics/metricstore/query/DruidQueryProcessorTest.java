/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.ebay.pulsar.analytics.datasource.ClientHelper;
import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.DataSourceTypeEnum;
import com.ebay.pulsar.analytics.datasource.DruidDataSourceProviderFactory;
import com.ebay.pulsar.analytics.datasource.DruidRestDBConnector;
import com.ebay.pulsar.analytics.datasource.DruidRestTableMeta;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.loader.StaticDataSourceConfigurationLoader;
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
import com.ebay.pulsar.analytics.metricstore.druid.query.DruidQueryProcessor;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.GroupByQuery;
import com.ebay.pulsar.analytics.query.request.SQLRequest;
import com.ebay.pulsar.analytics.query.response.TraceAbleResponse;
import com.ebay.pulsar.analytics.util.JsonUtil;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

@RunWith(PowerMockRunner.class)  //1
@PrepareForTest({ResteasyClientBuilder.class,ClientHelper.class,DataSourceMetaRepo.class})
public class DruidQueryProcessorTest {
	@Test
	public void testDruidQueryAll() throws Exception{
		mockDruidUnderlyingBase();
		mockDruidUnderlyingResponseStatus(Status.OK);
		mockTable("pulsar_session");
		initContext();
		
		String dataSourceName="trackingdruid";
		DruidQueryProcessor sqlRequestProcessor =new DruidQueryProcessor();
		sqlRequestProcessor.setCacheProvider(null);
		assertNull( sqlRequestProcessor.getCacheProvider());
		URL url = Thread.currentThread().getContextClassLoader().getResource("querys.txt");
		List<String> querys=Files.readLines(new File(url.getPath()), Charset.defaultCharset());
		url = Thread.currentThread().getContextClassLoader().getResource("raws.txt");
		List<String> raws=Files.readLines(new File(url.getPath()), Charset.defaultCharset());
		url = Thread.currentThread().getContextClassLoader().getResource("results.txt");
		List<String> results=Files.readLines(new File(url.getPath()), Charset.defaultCharset());
		
		for(int i=0;i<querys.size();i++){
			TraceAbleResponse expected=null;
			TraceAbleResponse resp=null;
			try{
				String query=querys.get(i);
				SQLRequest req=JsonUtil.readValue(query, SQLRequest.class);
				String raw=raws.get(i);
				String result=results.get(i);
				mockDruidTopNQueryResult(Status.OK,raw);
				mockDruidGroupByQueryResult(Status.OK,raw);
				mockDruidTimeSeriesQueryResult(Status.OK,raw);	
				expected=expected(result);
				resp = processResponse(sqlRequestProcessor.executeQuery(req, dataSourceName));
				assertEquals(resp.getQueryResult(), expected.getQueryResult());
				//assertEquals(resp.getQuery().getQuery(), expected.getQuery().getQuery());
			}catch(Throwable e){
				System.out.println("i="+i);
//				System.out.println("expected:"+JsonUtil.writeValueAsString(expected.getQuery().getQuery()));
//				System.out.println("result  :"+JsonUtil.writeValueAsString(resp.getQuery().getQuery()));
			}
		}
	}
	@Test
	public void testDruidQueryProcessor() throws Exception{
		mockDruidUnderlyingBase();
		mockDruidUnderlyingResponseStatus(Status.OK);
		mockTable("pulsar_session");
		initContext();
		String groupByResult="abaaba";
		mockDruidTopNQueryResult(Status.OK,"aaa");
		mockDruidGroupByQueryResult(Status.OK,groupByResult);
		mockDruidTimeSeriesQueryResult(Status.OK,"ccc");
		
		
		String endpoint="http://test";
		
		//String content="[\"pulsar_event_items\",\"pulsar_seo_attri\",\"pulsar_ogmb\",\"pulsar_ogmb_h\",\"pulsar_session_h\",\"pulsar_session\",\"pulsar_event\",\"epsession\",\"pulsar_ogmb_m\",\"pulsar_event_h\"]";
		Set<?> result=tablesContent;//JsonUtil.readValue(tablesContent, Set.class); 
		
		DataSourceConfiguration config=new DataSourceConfiguration(DataSourceTypeEnum.DRUID,"UTTEST");
		config.setEndPoint(Lists.newArrayList(endpoint));
		
		DruidRestDBConnector connector=new DruidRestDBConnector(config);
		Set<String> tables=connector.getAllTables();
		
		assertEquals(result,tables);
		
		Set<String> tables2=connector.getAllTables(endpoint);
		assertEquals(result,tables2);
		
		//String metaContent="{\"dimensions\":[\"uid\",\"region\",\"imgsearch\",\"browserfamily\",\"linespeed\",\"exitpage\",\"sojec\",\"plsec\",\"trafficsource\",\"dmgage\",\"dmgchn\",\"trafficsourceview\",\"osversion\",\"city\",\"clickct\",\"tenant\",\"cobrand\",\"dmgreg\",\"devicefamily\",\"dmgopc\",\"osfamily\",\"site\",\"app\",\"trafficsourceid\",\"entrypage\",\"deviceclass\",\"refdomain\",\"dmginl\",\"refdomainview\",\"country\",\"_bounce\",\"dmggd\",\"browserversion\",\"continent\",\"dmgmrg\",\"pagetype\",\"dmgct\",\"rv\",\"pagect\"],\"metrics\":[\"asqct_ag\",\"vict_ag\",\"count\",\"srpct_ag\",\"totaleventct_ag\",\"totalpagect_ag\",\"hpct_ag\",\"addtocartct_ag\",\"sessionduration_ag\",\"boct_ag\",\"bounce\",\"watchct_ag\",\"bidct_ag\",\"retvisitor\",\"binct_ag\",\"addtolistct_ag\"]}";
		DruidRestTableMeta metaResult=JsonUtil.readValue(metaContent, DruidRestTableMeta.class);
		when(response.readEntity(Mockito.eq(DruidRestTableMeta.class))).thenReturn(metaResult);
		
		Table t=connector.getTableMeta("pulsar_session");
		Table t2=connector.getTableMeta("pulsar_session", endpoint);
		assertEquals(t,t2);
		t=connector.getTableMeta(null);
		t2=connector.getTableMeta(null, endpoint);
		assertNull(t);
		assertNull(t2);

		String dataSource = "GroupByQueryTest";
		List<String> intervals = getIntervals();
		List<String> dimensions = getDimensions();
		List<BaseAggregator> aggregations = getAggregators();
		BaseGranularity granularity = BaseGranularity.ALL;
		//int limit = 10;
		// GroupByQuery with SimpleGranularity
		GroupByQuery groupByQuery = new GroupByQuery (dataSource, intervals, granularity, aggregations, dimensions);

		String groupByQueryResult=connector.sendQuery(groupByQuery);
		assertEquals(groupByResult,groupByQueryResult);
		String groupByQueryResult2=connector.sendQuery(groupByQuery, endpoint);
		assertEquals(groupByResult,groupByQueryResult2);
		
		Object obj=connector.query(groupByQuery);
		assertEquals(groupByResult,obj);
		
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
		mockDruidGroupByQueryResult(Status.BAD_REQUEST,groupByResult);
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
	
	private Client client;
	private WebTarget target;
	private Builder builder;
	private Response response;
	//private String tablesContent="[\"plsr_mplt_sessionevent\",\"pulsar_event_items\",\"plsr_mplt_event\",\"pulsar_seo_attri\",\"pulsar_ogmb\",\"pulsar_session\",\"pulsar_session_h\",\"pulsar_ogmb_h\",\"pulsar_event\",\"pulsar_ogmb_m\",\"epsession\",\"pulsar_event_h\"]";
	//private String tablesContent="[\"pulsar_session\",\"pulsar_event\"]";
	private Set<String> tablesContent=Sets.newHashSet("pulsar_session");
	private String metaContent="{\"dimensions\":[\"uid\",\"region\",\"imgsearch\",\"browserfamily\",\"linespeed\",\"exitpage\",\"sojec\",\"plsec\",\"trafficsource\",\"dmgage\",\"dmgchn\",\"trafficsourceview\",\"osversion\",\"city\",\"clickct\",\"tenant\",\"cobrand\",\"dmgreg\",\"devicefamily\",\"dmgopc\",\"osfamily\",\"site\",\"app\",\"trafficsourceid\",\"entrypage\",\"deviceclass\",\"refdomain\",\"dmginl\",\"refdomainview\",\"country\",\"_bounce\",\"dmggd\",\"browserversion\",\"continent\",\"dmgmrg\",\"pagetype\",\"dmgct\",\"rv\",\"pagect\"],\"metrics\":[\"asqct_ag\",\"vict_ag\",\"count\",\"srpct_ag\",\"totaleventct_ag\",\"totalpagect_ag\",\"hpct_ag\",\"addtocartct_ag\",\"sessionduration_ag\",\"boct_ag\",\"bounce\",\"watchct_ag\",\"bidct_ag\",\"retvisitor\",\"binct_ag\",\"addtolistct_ag\"]}";
	
	//private String plsr_mplt_sessionevent="{\"dimensions\":[\"uid\",\"site\",\"region\",\"browserfamily\",\"linespeed\",\"sojec\",\"plsec\",\"deviceclass\",\"trafficsource\",\"trafficsourceview\",\"refdomainview\",\"osversion\",\"country\",\"city\",\"_bounce\",\"clickct\",\"tenant\",\"browserversion\",\"continent\",\"rv\",\"devicefamily\",\"osfamily\",\"pagect\"],\"metrics\":[\"asqct_ag\",\"vict_ag\",\"count\",\"srpct_ag\",\"totaleventct_ag\",\"totalpagect_ag\",\"hpct_ag\",\"addtocartct_ag\",\"sessionduration_ag\",\"boct_ag\",\"watchct_ag\",\"bidct_ag\",\"binct_ag\",\"addtolistct_ag\"]}";
	//private String plsr_mplt_event="{\"dimensions\":[\"region\",\"linespeed\",\"browserfamily\",\"trafficsource\",\"dmgage\",\"dmgchn\",\"osversion\",\"city\",\"tenant\",\"dmgreg\",\"dmgopc\",\"devicefamily\",\"osfamily\",\"site\",\"deviceclass\",\"dmginl\",\"actionname\",\"familyname\",\"url\",\"socialsite\",\"country\",\"dmggd\",\"browserversion\",\"dmgmrg\",\"continent\",\"dmgct\",\"eventtype\"],\"metrics\":[\"guid_hll\",\"count\",\"dwell_ag\"]}";
	//private String pulsar_seo_attri="{\"dimensions\":[\"imgsearch\",\"region\",\"uid\",\"itm\",\"linespeed\",\"browserfamily\",\"dmgage\",\"dmgchn\",\"osversion\",\"trafficsourceview\",\"city\",\"ldpagename\",\"cobrand\",\"dmgreg\",\"devicefamily\",\"dmgopc\",\"osfamily\",\"site\",\"trafficsourceid\",\"landingpage\",\"deviceclass\",\"refdomain\",\"dmginl\",\"refdomainview\",\"country\",\"itemprice\",\"dmggd\",\"browserversion\",\"dmgmrg\",\"continent\",\"pagetype\",\"dmgct\"],\"metrics\":[\"session_hil\",\"dgmb_ag\",\"quantity_ag\",\"count\",\"reactivebuyer_ag\",\"newbuyer_ag\"]}";
	private String pulsar_ogmb="{\"dimensions\":[\"region\",\"itm\",\"browserfamily\",\"linespeed\",\"trafficsource\",\"dmgage\",\"dmgchn\",\"osversion\",\"city\",\"merchday\",\"_plmtid\",\"_cpgnname\",\"page\",\"dmgreg\",\"devicefamily\",\"dmgopc\",\"osfamily\",\"_cttname\",\"site\",\"_cpgnid\",\"deviceclass\",\"m\",\"dmginl\",\"_cttid\",\"country\",\"_cpgnpt\",\"itemprice\",\"dmggd\",\"browserversion\",\"_plmtname\",\"dmgmrg\",\"continent\",\"dmgct\",\"eventtype\"],\"metrics\":[\"clickcount_ag\",\"quantity_ag\",\"imprecount_ag\",\"count\",\"gmv_ag\",\"vicount_ag\"]}";
	private String pulsar_session="{\"dimensions\":[\"imgsearch\",\"region\",\"uid\",\"linespeed\",\"browserfamily\",\"exitpage\",\"plsec\",\"sojec\",\"trafficsource\",\"dmgage\",\"dmgchn\",\"trafficsourceview\",\"osversion\",\"city\",\"clickct\",\"tenant\",\"cobrand\",\"dmgreg\",\"devicefamily\",\"dmgopc\",\"osfamily\",\"site\",\"app\",\"trafficsourceid\",\"entrypage\",\"deviceclass\",\"refdomain\",\"dmginl\",\"refdomainview\",\"country\",\"dmggd\",\"browserversion\",\"dmgmrg\",\"continent\",\"pagetype\",\"dmgct\",\"pagect\"],\"metrics\":[\"asqct_ag\",\"vict_ag\",\"count\",\"srpct_ag\",\"totaleventct_ag\",\"totalpagect_ag\",\"hpct_ag\",\"addtocartct_ag\",\"sessionduration_ag\",\"boct_ag\",\"bounce\",\"watchct_ag\",\"bidct_ag\",\"retvisitor\",\"binct_ag\",\"addtolistct_ag\"]}";
	private String pulsar_event="{\"dimensions\":[\"region\",\"linespeed\",\"browserfamily\",\"trafficsource\",\"dmgage\",\"dmgchn\",\"prevpage\",\"osversion\",\"city\",\"tenant\",\"page\",\"dmgreg\",\"devicefamily\",\"dmgopc\",\"pagegroup\",\"osfamily\",\"site\",\"app\",\"deviceclass\",\"dmginl\",\"actionname\",\"familyname\",\"url\",\"socialsite\",\"country\",\"dmggd\",\"browserversion\",\"dmgmrg\",\"continent\",\"dmgct\",\"eventtype\"],\"metrics\":[\"guid_hll\",\"count\",\"dwell_ag\"]}";
	//private String pulsar_ogmb_m="{\"dimensions\":[\"region\",\"itm\",\"browserfamily\",\"linespeed\",\"trafficsource\",\"dmgage\",\"dmgchn\",\"osversion\",\"city\",\"merchday\",\"_plmtid\",\"_cpgnname\",\"page\",\"dmgreg\",\"devicefamily\",\"dmgopc\",\"osfamily\",\"_cttname\",\"site\",\"_cpgnid\",\"deviceclass\",\"m\",\"dmginl\",\"_cttid\",\"country\",\"_cpgnpt\",\"itemprice\",\"dmggd\",\"browserversion\",\"_plmtname\",\"dmgmrg\",\"continent\",\"dmgct\",\"eventtype\"],\"metrics\":[\"clickcount_ag\",\"quantity_ag\",\"imprecount_ag\",\"count\",\"gmv_ag\",\"vicount_ag\"]}";
	private String epsession="{\"dimensions\":[\"site\",\"region\",\"app\",\"linespeed\",\"browserfamily\",\"exitpage\",\"_nqt\",\"entrypage\",\"deviceclass\",\"trafficsource\",\"osversion\",\"ec\",\"country\",\"city\",\"clickct\",\"t\",\"tenant\",\"browserversion\",\"continent\",\"rv\",\"devicefamily\",\"osfamily\",\"pagect\"],\"metrics\":[\"asqct_ag\",\"vict_ag\",\"count\",\"srpct_ag\",\"totaleventct_ag\",\"totalpagect_ag\",\"viewct_ag\",\"hpct_ag\",\"addtocartct_ag\",\"sessionduration_ag\",\"boct_ag\",\"watchct_ag\",\"bidct_ag\",\"servct_ag\",\"binct_ag\",\"addtolistct_ag\"]}";
	private Map<String,String> tableMetas=ImmutableMap.of(
			"pulsar_ogmb", pulsar_ogmb, 
			"pulsar_session", pulsar_session,
			"pulsar_event",pulsar_event,
			"epsession",epsession);
	//private String dataResult="test string result.";
	private Response topNResponse;
	private Response timeSeriesResponse;
	private Response groupByResponse;

	private TraceAbleResponse processResponse(TraceAbleResponse response) throws IOException{
		String qs=JsonUtil.writeValueAsString(response);
		TraceAbleResponse exp=JsonUtil.readValue(qs,TraceAbleResponse.class);
		return exp;
	}
	private TraceAbleResponse expected(String expected) throws JsonParseException, JsonMappingException, IOException{
		TraceAbleResponse exp=JsonUtil.readValue(expected,TraceAbleResponse.class);
		return exp;
	}
	private void initContext(){
		String endpoint="http://test";
		DataSourceConfiguration config=new DataSourceConfiguration(DataSourceTypeEnum.DRUID,"UTTEST");
		config.setEndPoint(Lists.newArrayList(endpoint));
		DruidDataSourceProviderFactory.getInstance();
		StaticDataSourceConfigurationLoader loader=new StaticDataSourceConfigurationLoader();
		loader.load();
/*		DataSourceMetaRepo DataSourceMetaRepoIns = PowerMockito.mock(DataSourceMetaRepo.class);
		PowerMockito.mockStatic(DataSourceMetaRepo.class);
		when(DataSourceMetaRepo.getInstance()).thenReturn(DataSourceMetaRepoIns);*/
		DataSourceMetaRepo.getInstance().getAllDBMeta();
	}
	
	private void mockDruidUnderlyingBase() throws Exception{
		String endpoint="http://test";
		client=Mockito.mock(ResteasyClient.class);
		//PowerMockito.mockStatic(ClientBuilder.class);
		//when(ClientBuilder.newClient(Mockito.any(Configuration.class))).thenReturn(client);
		
		ResteasyClientBuilder easybuilder=Mockito.mock(ResteasyClientBuilder.class);
		PowerMockito.whenNew(ResteasyClientBuilder.class).withNoArguments().thenReturn(easybuilder);
		when(easybuilder.connectionPoolSize(20)).thenReturn(easybuilder);
		when(easybuilder.connectionTTL(2, TimeUnit.SECONDS)).thenReturn(easybuilder);
		when(easybuilder.establishConnectionTimeout(10,TimeUnit.SECONDS)).thenReturn(easybuilder);
		when(easybuilder.socketTimeout(60,TimeUnit.SECONDS)).thenReturn(easybuilder);
		when(easybuilder.build()).thenReturn((ResteasyClient)client);
		target=Mockito.mock(ResteasyWebTarget.class);
		when(client.target(Mockito.eq(endpoint))).thenReturn(target);
		
		when(target.path(Mockito.eq("datasources"))).thenReturn(target);
		builder = Mockito.mock(Builder.class);
		when(target.request(Mockito.eq(MediaType.APPLICATION_JSON_TYPE))).thenReturn(builder);
		response=Mockito.mock(Response.class);
		when(builder.get()).thenReturn(response);
		when(builder.post(Mockito.any(Entity.class))).thenReturn(response);
		when(response.readEntity(Mockito.eq(Map.class))).thenReturn(Maps.newHashMap());	
		when(response.readEntity(Mockito.eq(List.class))).thenReturn(Lists.newArrayList());		
//		when(response.getStatus()).thenReturn(Status.OK.getStatusCode());
//		Set result=JsonUtil.readValue(tablesContent, Set.class); 
//		when(response.readEntity(Mockito.eq(Set.class))).thenReturn(result);

//		when(target.path(Mockito.eq("pulsar_session"))).thenReturn(target);
//		DruidRestTableMeta metaResult=JsonUtil.readValue(metaContent, DruidRestTableMeta.class);
//		when(response.readEntity(Mockito.eq(DruidRestTableMeta.class))).thenReturn(metaResult);
		
		when(target.path(Mockito.eq("/"))).thenReturn(target);
		when(target.queryParam("pretty")).thenReturn(target);
		//when(builder.post(Mockito.any(Entity.class))).thenReturn(response);
		//when(response.readEntity(String.class)).thenReturn(dataResult);

		topNResponse=Mockito.mock(Response.class);
		when(builder.post(Matchers.argThat(new ArgumentMatcher<Entity<?>>(){
			@Override
			public boolean matches(Object argument) {
				if(argument!=null){
					Entity<?> entity=(Entity<?>)argument;
					try{
						@SuppressWarnings("unchecked")
						Map<String,Object> query=JsonUtil.readValue(entity.getEntity().toString(), Map.class);
						return query.get("queryType").toString().equals("topN");
					}catch(Exception e){
						
					}
					//return entity.getEntity().getClass().equals(TopNQuery.class);
				}
				return false;
			}
		})))
		.thenReturn(topNResponse);
		
		groupByResponse=Mockito.mock(Response.class);
		when(builder.post(Matchers.argThat(new ArgumentMatcher<Entity<?>>(){
			@Override
			public boolean matches(Object argument) {
				if(argument!=null){
					Entity<?> entity=(Entity<?>)argument;
					try{
						@SuppressWarnings("unchecked")
						Map<String,Object> query=JsonUtil.readValue(entity.getEntity().toString(), Map.class);
						return query.get("queryType").toString().equals("groupBy");
					}catch(Exception e){
						
					}
					//return entity.getEntity().getClass().equals(GroupByQuery.class);
				}
				return false;
				
			}
		})))
		.thenReturn(groupByResponse);
		
		timeSeriesResponse=Mockito.mock(Response.class);
		when(builder.post(Matchers.argThat(new ArgumentMatcher<Entity<?>>(){
			@Override
			public boolean matches(Object argument) {
				if(argument!=null){
					Entity<?> entity=(Entity<?>)argument;
					try{
						@SuppressWarnings("unchecked")
						Map<String,Object> query=JsonUtil.readValue(entity.getEntity().toString(), Map.class);
						return query.get("queryType").toString().equals("timeseries");
					}catch(Exception e){
						
					}
					//return entity.getEntity().getClass().equals(TimeSeriesQuery.class);
				}
				return false;
			}
		})))
		.thenReturn(timeSeriesResponse);
		
	}
	
	
	private void mockTable(String tableName) throws JsonParseException, JsonMappingException, IOException{
		
		when(response.getStatus()).thenReturn(Status.OK.getStatusCode());
		//Set result=JsonUtil.readValue(tablesContent, Set.class); 
		tablesContent.clear();
		tablesContent.add(tableName);
		when(response.readEntity(Mockito.eq(Set.class))).thenReturn(tablesContent);
		
		when(target.path(Mockito.eq(tableName))).thenReturn(target);
		DruidRestTableMeta metaResult=JsonUtil.readValue(tableMetas.get(tableName), DruidRestTableMeta.class);
		when(response.readEntity(Mockito.eq(DruidRestTableMeta.class))).thenReturn(metaResult);
	}
	
	private void mockDruidTopNQueryResult(Status status, String dataResult){
		when(topNResponse.getStatus()).thenReturn(status.getStatusCode());
		when(topNResponse.readEntity(String.class)).thenReturn(dataResult);
		
	}
	private void mockDruidGroupByQueryResult(Status status, String dataResult){
		when(groupByResponse.getStatus()).thenReturn(status.getStatusCode());
		when(groupByResponse.readEntity(String.class)).thenReturn(dataResult);
	}
	private void mockDruidTimeSeriesQueryResult(Status status, String dataResult){
		when(timeSeriesResponse.getStatus()).thenReturn(status.getStatusCode());
		when(timeSeriesResponse.readEntity(String.class)).thenReturn(dataResult);
	}
	private void mockDruidUnderlyingResponseStatus(Status status){
		when(response.getStatus()).thenReturn(status.getStatusCode());
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
