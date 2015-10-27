/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.metricstore.db;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;

import com.ebay.pulsar.analytics.dao.RDBMS;
import com.ebay.pulsar.analytics.dao.mapper.DBDashboardMapper;
import com.ebay.pulsar.analytics.dao.mapper.DBDataSourceMapper;
import com.ebay.pulsar.analytics.dao.model.DBDashboard;
import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.service.BaseDBService;
import com.ebay.pulsar.analytics.dao.service.DBDashboardService;
import com.ebay.pulsar.analytics.dao.service.DBDataSourceService;
import com.ebay.pulsar.analytics.dao.service.DBRightGroupService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class BaseDBServiceTest {
	public static final String uttestuser="uttesuser";
	public static final String uttestdatasource1="uttestdatasource1";
	public static final String uttestdatasource2="uttestdatasource2";
	@SuppressWarnings("unchecked")
	@Test
	public void testInsert() throws Exception{
		final DBDashboard dashboardCondition = new DBDashboard();
		dashboardCondition.setOwner("testUser");
		dashboardCondition.setName("UTDashboard1");
		DBDashboard dashboardCondition2 = new DBDashboard();
		dashboardCondition2.setOwner("testUser");
		dashboardCondition2.setName("UTDashboard2");
		
		RDBMS db = Mockito.mock(RDBMS.class);		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", "UTDashboard1")), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.<DBDashboard>newArrayList());
		
		when(db.insert(Mockito.anyString(), Matchers.anyMap(), Matchers.any(GeneratedKeyHolder.class)))
		.thenReturn(0);
		
		final GeneratedKeyHolder keyHolder = PowerMockito.mock(GeneratedKeyHolder.class);	
		PowerMockito.whenNew(GeneratedKeyHolder.class).withNoArguments()
		.thenReturn(keyHolder);
		when(keyHolder.getKey()).thenReturn(1L);
		DBDashboardService dashboardService = new DBDashboardService();

		ReflectFieldUtil.setField(BaseDBService.class,dashboardService, "db", db);

		assertTrue(dashboardService.inser(dashboardCondition)==-1);

	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testDelete() throws Exception{
		final DBDataSource datasourceCondition = new DBDataSource();
		datasourceCondition.setName(uttestdatasource1);		
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
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("id",1L))))
		.thenReturn(1);
		DBDataSourceService datasourceService = new DBDataSourceService();
		ReflectFieldUtil.setField(BaseDBService.class,datasourceService, "db", db);

		long id = datasourceService.deleteBatch(datasourceCondition);
		Assert.assertTrue(id > 0);
		
		long id2 = datasourceService.deleteById(1L);
		Assert.assertTrue(id2 > 0);

	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testUpdate() throws Exception{
		DBDataSource datasource = new DBDataSource();
		datasource.setName(uttestdatasource1);
		
		final DBDataSource datasourceCondition = new DBDataSource();
		datasourceCondition.setName(uttestdatasource1);
		datasourceCondition.setCreateTime(new Date());
		RDBMS db = Mockito.mock(RDBMS.class);		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdatasource1)), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))
		.thenReturn(Lists.<DBDataSource>newArrayList(datasourceCondition));

		when(db.update(Mockito.anyString(), Matchers.anyMap()))
		.thenReturn(1);
		
		DBDataSourceService datasourceService = new DBDataSourceService();
		ReflectFieldUtil.setField(BaseDBService.class,datasourceService, "db", db);
		
		String newConfig = "testConfig";
		datasource.setProperties(newConfig);
		long id = datasourceService.updateById(datasourceCondition);
		Assert.assertTrue(id == 1);
	}
	
	@Test
	public void testGet() throws Exception{
		DBDataSource datasource = new DBDataSource();
		datasource.setName(uttestdatasource1);
		datasource.setId(1L);
		datasource.setOwner(uttestuser);
		
		List<DBDataSource> sample=Lists.newArrayList(datasource);
		sample.add(datasource);
		
		RDBMS db = Mockito.mock(RDBMS.class);		
		when(db.query(Matchers.anyString(),Matchers.any(MapSqlParameterSource.class), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))
		.thenReturn(sample);
		
		DBDataSourceService datasourceService = new DBDataSourceService();
		ReflectFieldUtil.setField(BaseDBService.class,datasourceService, "db", db);
		
		String newConfig = "testConfig";
		datasource.setProperties(newConfig);
		Assert.assertTrue(datasourceService.get(datasource, 10).size()==0);
	}
	
	
	@Test
	public void testGetColumn() throws Exception{
		DBDataSource datasource = new DBDataSource();
		datasource.setName(uttestdatasource1);
		datasource.setId(1L);
		datasource.setOwner(uttestuser);
		
		List<DBDataSource> sample=Lists.newArrayList(datasource);
		sample.add(datasource);
		
		RDBMS db = Mockito.mock(RDBMS.class);		
		when(db.query(Matchers.anyString(),Matchers.any(MapSqlParameterSource.class), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))
		.thenReturn(sample);
		DBDataSourceService datasourceService = new DBDataSourceService();
		ReflectFieldUtil.setField(BaseDBService.class,datasourceService, "db", db);
		
		String newConfig = "testConfig";
		datasource.setProperties(newConfig);
		Assert.assertTrue(datasourceService.getAllByColumnIn("column", Lists.newArrayList("test"), -1).size()>0);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testDBRightGroupService() throws Exception{
		DBRightGroupService rightGroupService=new DBRightGroupService();
		RDBMS db = Mockito.mock(RDBMS.class);		
		when(db.update(Matchers.anyString(),Matchers.any(Map.class)))
		.thenReturn(1);
		ReflectFieldUtil.setField(BaseDBService.class,rightGroupService, "db", db);
		assertTrue(rightGroupService.deleteRightsFromGroupByPrefix("test_")>0);
	}

}
