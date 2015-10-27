/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.ebay.pulsar.analytics.dao.RDBMS;
import com.ebay.pulsar.analytics.dao.mapper.DBDashboardMapper;
import com.ebay.pulsar.analytics.dao.mapper.DBDataSourceMapper;
import com.ebay.pulsar.analytics.dao.mapper.DBGroupMapper;
import com.ebay.pulsar.analytics.dao.model.DBDashboard;
import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.model.DBGroup;
import com.ebay.pulsar.analytics.dao.service.BaseDBService;
import com.ebay.pulsar.analytics.dao.service.DirectSQLAccessService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(PowerMockRunner.class)  //1
@PrepareForTest({GeneratedKeyHolder.class,BaseDBService.class})
public class UserPermissionControlTest {
	private UserPermissionControl control=new UserPermissionControl();
	public static final String uttestuser="uttestqxing";
	public static final String uttestdatasource1="uttestdatasource1";
	public static final String uttestdatasource2="uttestdatasource2";
	public static final String uttestdashbaord1="uttestdashbaord1";
	public static final String uttestdashbaord2="uttestdashbaord2";
	public static final String uttestgroup1="uttestgroup1";
	public static final String uttestgroup2="uttestgroup2";
	public static final String driver2="com.mysql.jdbc.Driver";
	public static final String url="jdbc:mysql://10.64.219.221:3306/pulsario";
	public static final String userName="root";
	public static final String userPwd="";
	
	public static class DBGroupMatcher extends ArgumentMatcher<DBGroupMapper>{
	     public boolean matches(Object obj) {
	         return obj.getClass().equals(DBGroupMapper.class);
	     }
	     public String toString() {
	         //printed in verification errors
	         return "[DBGroupMapper]";
	     }
	 }
	public static class DBDataSourceMatcher extends ArgumentMatcher<DBDataSourceMapper>{
	     public boolean matches(Object obj) {
	         return obj.getClass().equals(DBDataSourceMapper.class);
	     }
	     public String toString() {
	         //printed in verification errors
	         return "[DBDataSourceMapper]";
	     }
	 }
	public static class DBDashboardMatcher extends ArgumentMatcher<DBDashboardMapper>{
	     public boolean matches(Object obj) {
	         return obj.getClass().equals(DBDashboardMapper.class);
	     }
	     public String toString() {
	         //printed in verification errors
	         return "[DBDashboardMapper]";
	     }
	 }	
    @Test
    public void testDb() throws Exception{
    	RDBMS db = Mockito.mock(RDBMS.class);		
		final GeneratedKeyHolder keyHolder = PowerMockito.mock(GeneratedKeyHolder.class);	
		PowerMockito.whenNew(GeneratedKeyHolder.class).withNoArguments()
		.thenReturn(keyHolder);
		when(keyHolder.getKey()).thenReturn(1L).thenReturn(2L);
		GroupService gs=(GroupService)ReflectFieldUtil.getField(control, "groupService");
		BaseDBService<?> ggs=(BaseDBService<?>)ReflectFieldUtil.getField(gs, "groupService");
		BaseDBService<?> grgs=(BaseDBService<?>)ReflectFieldUtil.getField(gs, "rightGroupService");
		BaseDBService<?> gugs=(BaseDBService<?>)ReflectFieldUtil.getField(gs, "userGroupService");
		BaseDBService<?> gus=(BaseDBService<?>)ReflectFieldUtil.getField(gs, "userService");
		ReflectFieldUtil.setField(BaseDBService.class,ggs, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,grgs, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,gugs, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,gus, "db", db);
		
		BaseDBService<?> rgs=(BaseDBService<?>)ReflectFieldUtil.getField(control, "rightGroupService");
		ReflectFieldUtil.setField(BaseDBService.class,rgs, "db", db);
				
		DashboardService ds=(DashboardService)ReflectFieldUtil.getField(control, "dashboardService");
		BaseDBService<?> dds=(BaseDBService<?>)ReflectFieldUtil.getField(ds, "dashboardService");
		BaseDBService<?> drgs=(BaseDBService<?>)ReflectFieldUtil.getField(ds, "rightGroupService");
		ReflectFieldUtil.setField(BaseDBService.class,dds, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,drgs, "db", db);
		
		DataSourceService dss=(DataSourceService)ReflectFieldUtil.getField(control, "datasourceService");
		BaseDBService<?> dsds=(BaseDBService<?>)ReflectFieldUtil.getField(dss, "datasourceService");
		BaseDBService<?> dsrgs=(BaseDBService<?>)ReflectFieldUtil.getField(dss, "rightGroupService");
		ReflectFieldUtil.setField(BaseDBService.class,dsds, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,dsrgs, "db", db);
		
		DirectSQLAccessService dqs=(DirectSQLAccessService)ReflectFieldUtil.getField(control, "directSQLAccessService");
		ReflectFieldUtil.setField(DirectSQLAccessService.class,dqs, "db", db);

		when(db.queryForList(Matchers.anyString(), Matchers.eq(ImmutableMap.of("userName","qxing")), Matchers.eq(-1)))
		.thenReturn(Lists.<Object>newArrayList("D1_VIEW","D2_MANAGE","ADD_DATASOURCE"));
		Set<SimpleGrantedAuthority> rights=control.getAllRightsForValidUser("qxing");
		Set<SimpleGrantedAuthority> sample=Sets.newHashSet(
				new SimpleGrantedAuthority("D1_VIEW"),
				new SimpleGrantedAuthority("D2_VIEW"),
				new SimpleGrantedAuthority("D2_MANAGE"),
				new SimpleGrantedAuthority("ADD_DATASOURCE")
				);
		
		Assert.assertEquals(sample, rights);
		
		DBDataSource datasourceCondition = new DBDataSource();
		datasourceCondition.setOwner(uttestuser);
		datasourceCondition.setName(uttestdatasource1);
		datasourceCondition.setType("druid");
		datasourceCondition.setEndpoint("http://endpoint.test.com");
		DBDataSource datasourceCondition2 = new DBDataSource();
		datasourceCondition2.setOwner(uttestuser);
		datasourceCondition2.setName(uttestdatasource2);
		datasourceCondition2.setType("druid");
		datasourceCondition2.setEndpoint("http://endpoint.test.com");
		
		DBDashboard dashboardCondition = new DBDashboard();
		dashboardCondition.setOwner(uttestuser);
		dashboardCondition.setName(uttestdashbaord1);
		DBDashboard dashboardCondition2 = new DBDashboard();
		dashboardCondition2.setOwner(uttestuser);
		dashboardCondition2.setName("UTDashboard2");
		
		DBGroup group1 = new DBGroup();
    	group1.setOwner(uttestuser);
    	group1.setName(uttestgroup1);
    	group1.setDisplayName("DisplayName1");
    
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("owner", uttestuser)), Matchers.eq(-1),Matchers.argThat(new ArgumentMatcher<DBGroupMapper>(){

			@Override
			public boolean matches(Object argument) {
				return argument.getClass().equals(DBGroupMapper.class);
			}
			
		})))
		.thenReturn(Lists.newArrayList(group1));
		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("owner", uttestuser)), Matchers.eq(-1),Matchers.argThat(new ArgumentMatcher<DBDashboardMapper>(){

			@Override
			public boolean matches(Object argument) {
				return argument.getClass().equals(DBDashboardMapper.class);
			}
			
		})))
		.thenReturn(Lists.newArrayList(dashboardCondition,dashboardCondition2));
		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("owner", uttestuser)), Matchers.eq(-1),Matchers.argThat(new ArgumentMatcher<DBDataSourceMapper>(){

			@Override
			public boolean matches(Object argument) {
				return argument.getClass().equals(DBDataSourceMapper.class);
			}
			
		})))
		.thenReturn(Lists.newArrayList(datasourceCondition,datasourceCondition2));

		List<String> allRightsForOwner=control.getAllRightsForOwner(uttestuser);
		System.out.println(allRightsForOwner);
		
		control.getAllRightsByUserName("qxing");

    }
    
    
}
