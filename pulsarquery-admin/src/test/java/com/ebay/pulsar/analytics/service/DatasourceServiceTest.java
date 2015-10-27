/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;		
		
import static org.mockito.Mockito.when;		
		





import java.util.Date;		
import java.util.List;		
import java.util.Map;		
import java.util.Set;		
		





import org.junit.Assert;		
import org.junit.Before;		
import org.junit.Test;		
import org.junit.runner.RunWith;		
import org.mockito.Matchers;		
import org.mockito.Mockito;		
import org.powermock.api.mockito.PowerMockito;		
import org.powermock.core.classloader.annotations.PrepareForTest;		
import org.powermock.modules.junit4.PowerMockRunner;		
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;		
import org.springframework.jdbc.support.GeneratedKeyHolder;		
		





import com.ebay.pulsar.analytics.dao.RDBMS;		
import com.ebay.pulsar.analytics.dao.mapper.DBDataSourceMapper;		
import com.ebay.pulsar.analytics.dao.model.DBDataSource;		
import com.ebay.pulsar.analytics.dao.service.BaseDBService;		
import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;		
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;		
import com.ebay.pulsar.analytics.datasource.DataSourceProvider;		
import com.ebay.pulsar.analytics.datasource.DataSourceTypeEnum;		
import com.ebay.pulsar.analytics.datasource.Table;		
import com.google.common.collect.ImmutableMap;		
import com.google.common.collect.Lists;		
import com.google.common.collect.Maps;		
import com.google.common.collect.Sets;		
		
@RunWith(PowerMockRunner.class)  //1		
@PrepareForTest({GeneratedKeyHolder.class,BaseDBService.class})		
public class DatasourceServiceTest {		
			
	public static final String uttestuser="uttestqxing";		
	public static final String uttestdatasource1="uttestdatasource1";		
	public static final String uttestdatasource2="uttestdatasource2";		
	public static final String driver2="com.mysql.jdbc.Driver";		
	public static final String url="jdbc:mysql://10.64.219.221:3306/pulsario";		
	public static final String userName="root";		
	public static final String userPwd="";		
	@Before		
	public void setup(){		
		
	}		
		
	@SuppressWarnings("unchecked")
	@Test		
	public void addDataSourceTest() throws Exception {		
		final DBDataSource datasourceCondition = new DBDataSource();		
		datasourceCondition.setOwner(uttestuser);		
		datasourceCondition.setName(uttestdatasource1);		
		datasourceCondition.setType("druid");		
		datasourceCondition.setEndpoint("http://endpoint.test.com");		
		DBDataSource datasourceCondition2 = new DBDataSource();		
		datasourceCondition2.setOwner(uttestuser);		
		datasourceCondition2.setName(uttestdatasource2);		
		datasourceCondition2.setType("druid");		
		datasourceCondition2.setEndpoint("http://endpoint.test.com");		
				
		RDBMS db = Mockito.mock(RDBMS.class);				
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdatasource1)), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))		
		.thenReturn(Lists.<DBDataSource>newArrayList());		
				
		when(db.insert(Mockito.anyString(), Matchers.anyMap(), Matchers.any(GeneratedKeyHolder.class)))		
		.thenReturn(1);		
				
		final GeneratedKeyHolder keyHolder = PowerMockito.mock(GeneratedKeyHolder.class);			
		PowerMockito.whenNew(GeneratedKeyHolder.class).withNoArguments()		
		.thenReturn(keyHolder);		
		when(keyHolder.getKey()).thenReturn(1L);		
		DataSourceService datasourceService = new DataSourceService();		
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "datasourceService");		
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "rightGroupService");		
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);		
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);		
				
		long id = datasourceService.addDataSource(datasourceCondition);		
		Assert.assertTrue(id > 0);		
				
				
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdatasource1)), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))		
		.thenReturn(Lists.<DBDataSource>newArrayList(datasourceCondition));		
				
		Assert.assertTrue(uttestuser.equalsIgnoreCase(datasourceService		
				.getDataSourceByName(uttestdatasource1).getOwner()));		
				
		long id2 = datasourceService.addDataSource(datasourceCondition2);		
		Assert.assertTrue(id2 > 0);		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("owner", uttestuser)), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))		
		.thenReturn(Lists.<DBDataSource>newArrayList(datasourceCondition,datasourceCondition2));		
		List<String> list = datasourceService		
				.getAllDataSourcesForOwner(uttestuser);		
				
		Assert.assertTrue(list.size() > 0);		
		Assert.assertTrue(list.size()==2);		
	}		
		
	@SuppressWarnings("unchecked")
	@Test		
	public void updateDataSourceTest() throws Exception {		
		DBDataSource datasource = new DBDataSource();		
		datasource.setName(uttestdatasource1);		
				
		final DBDataSource datasourceCondition = new DBDataSource();		
		datasourceCondition.setOwner(uttestuser);		
		datasourceCondition.setName(uttestdatasource1);		
		datasourceCondition.setType("druid");		
		datasourceCondition.setEndpoint("http://endpoint.test.com");		
		datasourceCondition.setCreateTime(new Date());		
		RDBMS db = Mockito.mock(RDBMS.class);				
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdatasource1)), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))		
		.thenReturn(Lists.<DBDataSource>newArrayList(datasourceCondition));		
		
		when(db.update(Mockito.anyString(), Matchers.anyMap()))		
		.thenReturn(1);		
				
		DataSourceService datasourceService = new DataSourceService();		
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "datasourceService");		
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "rightGroupService");		
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);		
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);		
				
		String newConfig = "testConfig";		
		datasource.setProperties(newConfig);		
		long id = datasourceService.updateDataSource(datasource);		
		Assert.assertTrue(id == 1);		
				
	}		
		
	@SuppressWarnings("unchecked")
	@Test		
	public void deleteDataSourceTest() throws Exception {		
		final DBDataSource datasourceCondition = new DBDataSource();		
		datasourceCondition.setOwner(uttestuser);		
		datasourceCondition.setName(uttestdatasource1);		
		datasourceCondition.setType("druid");		
		datasourceCondition.setEndpoint("http://endpoint.test.com");		
		datasourceCondition.setCreateTime(new Date());		
				
		final DBDataSource datasourceCondition2 = new DBDataSource();		
		datasourceCondition2.setOwner(uttestuser);		
		datasourceCondition2.setName(uttestdatasource2);		
		datasourceCondition2.setType("druid");		
		datasourceCondition2.setEndpoint("http://endpoint.test.com");		
		datasourceCondition2.setCreateTime(new Date());		
				
		RDBMS db = Mockito.mock(RDBMS.class);				
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdatasource1)), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))		
		.thenReturn(Lists.<DBDataSource>newArrayList(datasourceCondition));		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdatasource2)), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))		
		.thenReturn(Lists.<DBDataSource>newArrayList(datasourceCondition2));		
				
		when(db.update(Mockito.anyString(), Matchers.anyMap()))		
		.thenReturn(1);		
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("name",uttestdatasource1))))		
		.thenReturn(1);		
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("name",uttestdatasource2))))		
		.thenReturn(1);		
		DataSourceService datasourceService = new DataSourceService();		
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "datasourceService");		
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "rightGroupService");		
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);		
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);		
		
		long id = datasourceService.deleteDataSource(uttestdatasource1);		
		Assert.assertTrue(id > 0);		
				
		datasourceService.deleteDataSources(Lists.newArrayList(		
				uttestdatasource1,uttestdatasource2));		
		Assert.assertTrue(id > 0);		
				
	}		
	@SuppressWarnings("unchecked")
	@Test		
	public void testOthers() throws Exception{		
		DBDataSource datasource = new DBDataSource();		
		datasource.setName(uttestdatasource1);		
		datasource.setId(1L);		
		datasource.setOwner(uttestuser);		
				
		List<DBDataSource> sample=Lists.newArrayList(datasource);		
				
		RDBMS db = Mockito.mock(RDBMS.class);				
		when(db.query(Matchers.anyString(),Matchers.any(MapSqlParameterSource.class), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))		
		.thenReturn(sample);		
				
		DataSourceService datasourceService = new DataSourceService();		
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "datasourceService");		
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "rightGroupService");		
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);		
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);		
				
		ImmutableMap.of("INPARAMETER", Sets.newHashSet(uttestdatasource1));		
		List<DBDataSource> r1=datasourceService.getDataSourceByNames(Lists.newArrayList(uttestdatasource1));		
		Assert.assertEquals(sample, r1);		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))		
		.thenReturn(sample);		
				
				
				
		DataSourceMetaRepo repo=Mockito.mock(DataSourceMetaRepo.class);		
		Map<String,DataSourceConfiguration> repoMap=Maps.newHashMap();		
		DataSourceConfiguration conf=new DataSourceConfiguration(DataSourceTypeEnum.PULSAR,"pulsar");		
		conf.setEndPoint(Lists.newArrayList("aaaa,bbbb"));		
		conf.setRealOnly(true);		
		conf.setRefreshTime(new Date().getTime());		
		repoMap.put("pulsar", conf);		
		when(repo.getDbConfMap()).thenReturn(repoMap);		
				
		ReflectFieldUtil.setStaticField(DataSourceMetaRepo.class, "instance", repo);		
		Set<DBDataSource> list=datasourceService.getAllUserManagedDatasource();//(uttestuser);		
		//Set<DBDataSource> setSample=Sets.newHashSet(sample);		
		Assert.assertTrue(list.size()==2);		
				
		when(repo.getActiveDbConfMap()).thenReturn(repoMap);		
				
		Set<DBDataSource> list2=datasourceService.getAllUserViewedDatasource();		
		Assert.assertTrue(list2.size()==2);		
	}		
	@Test		
	public void testGetTables() throws Exception{		
				
		DataSourceProvider provider=Mockito.mock(DataSourceProvider.class);		
		List<Table> tables=Lists.newArrayList();		
		Table t=new Table();		
		t.setDateColumn("time");		
		t.setTableName("T1");		
		tables.add(t);		
		when(provider.getTables()).thenReturn(tables);		
		DataSourceMetaRepo repo=Mockito.mock(DataSourceMetaRepo.class);		
		Map<String,DataSourceConfiguration> repoMap=Maps.newHashMap();		
		DataSourceConfiguration conf=new DataSourceConfiguration(DataSourceTypeEnum.PULSAR,"pulsar");		
		conf.setEndPoint(Lists.newArrayList("aaaa,bbbb"));		
		conf.setRealOnly(true);		
		conf.setRefreshTime(new Date().getTime());		
		repoMap.put("pulsar", conf);		
		when(repo.getDBMetaFromCache(Matchers.anyString())).thenReturn(provider);		
		ReflectFieldUtil.setStaticField(DataSourceMetaRepo.class, "instance", repo);		
		
				
		RDBMS db = Mockito.mock(RDBMS.class);				
		DataSourceService datasourceService = new DataSourceService();		
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "datasourceService");		
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(datasourceService, "rightGroupService");		
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);		
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);		
				
		DBDataSource datasource = new DBDataSource();		
		datasource.setName(uttestdatasource1);		
		datasource.setId(1L);		
		datasource.setOwner(uttestuser);		
		datasource.setType(DataSourceTypeEnum.DRUID.getType());		
		List<String> ts=datasourceService.getDataSourceTables(datasource);		
		Assert.assertEquals(Lists.newArrayList("T1"), ts);		
	}		
		
}