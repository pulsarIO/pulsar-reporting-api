/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;

import static org.mockito.Mockito.when;

import java.util.List;

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
import com.ebay.pulsar.analytics.dao.mapper.DBDashboardMapper;
import com.ebay.pulsar.analytics.dao.model.DBDashboard;
import com.ebay.pulsar.analytics.dao.service.BaseDBService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(PowerMockRunner.class)  //1
@PrepareForTest({GeneratedKeyHolder.class,BaseDBService.class})
public class DashboardServiceTest {
	
	public static final String uttestuser="uttestqxing";
	public static final String uttestdashbaord1="uttestdashbaord1";
	public static final String uttestdashbaord2="uttestdashbaord2";
	public static final String driver2="com.mysql.jdbc.Driver";
	public static final String url="jdbc:mysql://10.64.219.221:3306/pulsario";
	public static final String userName="root";
	public static final String userPwd="";
	@Before
	public void setup(){

	}

	@SuppressWarnings("unchecked")
	@Test
	public void addDashboardTest() throws Exception {
		final DBDashboard dashboardCondition = new DBDashboard();
		dashboardCondition.setOwner(uttestuser);
		dashboardCondition.setName(uttestdashbaord1);
		DBDashboard dashboardCondition2 = new DBDashboard();
		dashboardCondition2.setOwner(uttestuser);
		dashboardCondition2.setName("UTDashboard2");
		
		RDBMS db = Mockito.mock(RDBMS.class);		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdashbaord1)), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.<DBDashboard>newArrayList());
		
		when(db.insert(Mockito.anyString(), Matchers.anyMap(), Matchers.any(GeneratedKeyHolder.class)))
		.thenReturn(1);
		
		final GeneratedKeyHolder keyHolder = PowerMockito.mock(GeneratedKeyHolder.class);	
		PowerMockito.whenNew(GeneratedKeyHolder.class).withNoArguments()
		.thenReturn(keyHolder);
		when(keyHolder.getKey()).thenReturn(1L);
		DashboardService dashboardService = new DashboardService();
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(dashboardService, "dashboardService");
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(dashboardService, "rightGroupService");
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);

		when(db.update(Mockito.anyString(), Matchers.anyMap())).thenReturn(1);
		long id = dashboardService.addDashboard(dashboardCondition);
		Assert.assertTrue(id > 0);
		Assert.assertEquals(String.format(PermissionConst.RESOURCE_NAME_TEMPLATGE, uttestdashbaord1,1L),dashboardCondition.getName());
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdashbaord1)), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.<DBDashboard>newArrayList(dashboardCondition));
		when(db.update(Matchers.anyString(),Matchers.anyMap()))
		.thenReturn(1);
		Assert.assertTrue(uttestuser.equalsIgnoreCase(dashboardService
				.getDashboardByName(uttestdashbaord1).getOwner()));
		
		long id2 = dashboardService.addDashboard(dashboardCondition2);
		Assert.assertTrue(id2 > 0);
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("owner", uttestuser)), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.<DBDashboard>newArrayList(dashboardCondition,dashboardCondition2));
		List<String> list = dashboardService
				.getAllDashboardsForOwner(uttestuser);
		
		Assert.assertTrue(list.size() > 0);
		Assert.assertTrue(list.size()==2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void updateDashboardTest() throws Exception {
		DBDashboard dashboard = new DBDashboard();
		dashboard.setName(uttestdashbaord1);
		dashboard.setId(1L);
		dashboard.setOwner(uttestuser);
		RDBMS db = Mockito.mock(RDBMS.class);		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdashbaord1)), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.<DBDashboard>newArrayList(dashboard));

		when(db.update(Mockito.anyString(), Matchers.anyMap()))
		.thenReturn(1);
		
		DashboardService dashboardService = new DashboardService();
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(dashboardService, "dashboardService");
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(dashboardService, "rightGroupService");
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);
		
		String newConfig = "testConfig";
		long id = dashboardService.updateDashboard(uttestdashbaord1, "newName", newConfig);
		Assert.assertTrue(id == 1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void deleteDashboardTest() throws Exception {
		
		RDBMS db = Mockito.mock(RDBMS.class);		
//		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestdashbaord1)), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
//		.thenReturn(Lists.<DBDashboard>newArrayList(dashboard));

		when(db.update(Mockito.anyString(), Matchers.anyMap()))
		.thenReturn(1);
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("name",uttestdashbaord1))))
		.thenReturn(1);
		DashboardService dashboardService = new DashboardService();
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(dashboardService, "dashboardService");
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(dashboardService, "rightGroupService");
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);
		//List<String> dashbaordNames=Lists.newArrayList("DruidDemo1","DruidDemo2");
		long id = dashboardService.deleteDashboard(uttestdashbaord1);
		Assert.assertTrue(id > 0);
		
		dashboardService.deleteDashboards(Lists.newArrayList(
				uttestdashbaord1));
		Assert.assertTrue(id > 0);
		
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testOthers() throws Exception{
		DBDashboard dashboard = new DBDashboard();
		dashboard.setName(uttestdashbaord1);
		dashboard.setId(1L);
		dashboard.setOwner(uttestuser);
		
		List<DBDashboard> sample=Lists.newArrayList(dashboard);
		
		RDBMS db = Mockito.mock(RDBMS.class);		
		when(db.query(Matchers.anyString(),Matchers.any(MapSqlParameterSource.class), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(sample);
		
		DashboardService dashboardService = new DashboardService();
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(dashboardService, "dashboardService");
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(dashboardService, "rightGroupService");
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);
		
		ImmutableMap.of("INPARAMETER", Sets.newHashSet(uttestdashbaord1));
		List<DBDashboard> r1=dashboardService.getDashboardByNames(Lists.newArrayList(uttestdashbaord1));
		Assert.assertEquals(sample, r1);
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.<DBDashboard>newArrayList(dashboard));
		
		List<DBDashboard> list=dashboardService.getAllUserManagedDashboard();
		Assert.assertEquals(sample, list);
	
		List<DBDashboard> list2=dashboardService.getUserViewedDashboard();
		Assert.assertEquals(sample, list2);
	}

}
