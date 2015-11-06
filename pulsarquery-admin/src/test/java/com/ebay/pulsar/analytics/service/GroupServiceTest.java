/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ebay.pulsar.analytics.service;

import static org.mockito.Mockito.when;

import java.util.Date;
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
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;

import com.ebay.pulsar.analytics.dao.RDBMS;
import com.ebay.pulsar.analytics.dao.mapper.DBDashboardMapper;
import com.ebay.pulsar.analytics.dao.mapper.DBDataSourceMapper;
import com.ebay.pulsar.analytics.dao.mapper.DBGroupMapper;
import com.ebay.pulsar.analytics.dao.mapper.DBRightGroupMapper;
import com.ebay.pulsar.analytics.dao.mapper.DBUserGroupMapper;
import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.model.DBGroup;
import com.ebay.pulsar.analytics.dao.model.DBRightGroup;
import com.ebay.pulsar.analytics.dao.model.DBUserGroup;
import com.ebay.pulsar.analytics.dao.service.BaseDBService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(PowerMockRunner.class)  //1
@PrepareForTest({GeneratedKeyHolder.class,BaseDBService.class})
public class GroupServiceTest {
	//private GroupService us=new GroupService();
	public static final String uttestuser="uttestqxing";
	public static final String uttestgroup1="uttestgroup1";
	public static final String uttestgroup2="uttestgroup2";
	public static final String userName="root";
	public static final String userPwd="";
	public static  DBGroup group1;
	public static  DBGroup group2;
    @SuppressWarnings("unchecked")
	@Test
    public void testDb() throws Exception{
    	group1 = new DBGroup();
    	group1.setOwner(uttestuser);
    	group1.setName(uttestgroup1);
    	group1.setDisplayName("DisplayName1");
    	group2 = new DBGroup();
    	group2.setOwner(uttestuser);
    	group2.setName(uttestgroup2);
    	group2.setDisplayName("DisplayName2");
		
		RDBMS db = Mockito.mock(RDBMS.class);		
		final GeneratedKeyHolder keyHolder = PowerMockito.mock(GeneratedKeyHolder.class);	
		PowerMockito.whenNew(GeneratedKeyHolder.class).withNoArguments()
		.thenReturn(keyHolder);
		when(keyHolder.getKey()).thenReturn(1L);
		GroupService groupService = new GroupService();
		BaseDBService<?> ds=(BaseDBService<?>)ReflectFieldUtil.getField(groupService, "groupService");
		BaseDBService<?> rs=(BaseDBService<?>)ReflectFieldUtil.getField(groupService, "rightGroupService");
		BaseDBService<?> ugs=(BaseDBService<?>)ReflectFieldUtil.getField(groupService, "userGroupService");
		BaseDBService<?> us=(BaseDBService<?>)ReflectFieldUtil.getField(groupService, "userService");
		BaseDBService<?> dss=(BaseDBService<?>)ReflectFieldUtil.getField(groupService, "dataSourceService");
		BaseDBService<?> dbs=(BaseDBService<?>)ReflectFieldUtil.getField(groupService, "dashboardService");
		ReflectFieldUtil.setField(BaseDBService.class,ds, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,rs, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,ugs, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,us, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,dss, "db", db);
		ReflectFieldUtil.setField(BaseDBService.class,dbs, "db", db);
		//mock add
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestgroup1)), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.<DBGroup>newArrayList());
		when(db.insert(Mockito.anyString(), Matchers.anyMap(), Matchers.any(GeneratedKeyHolder.class)))
		.thenReturn(1);
		long id1=groupService.addGroup(group1);
		Assert.assertTrue(id1>0);
		
		DBRightGroup rightGroup=new DBRightGroup();
		String rightName="datasource1_MANAGE";
		rightGroup.setGroupName(uttestgroup1);
		rightGroup.setRightName(rightName);
		rightGroup.setRightType(PermissionConst.RIGHT_TYPE_DATA);
		rightGroup.setCreateTime(new Date());
		rightGroup.setLastUpdateTime(new Date());
		List<DBRightGroup> dbRightGroups=Lists.newArrayList(rightGroup);
		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.argThat(new ArgumentMatcher<DBRightGroupMapper>(){

			@Override
			public boolean matches(Object argument) {
				return argument.getClass().equals(DBRightGroupMapper.class);
			}
			
		})))
		.thenReturn(Lists.<DBRightGroup>newArrayList());
		when(db.insert(Mockito.anyString(), Matchers.anyMap(), Matchers.any(GeneratedKeyHolder.class)))
		.thenReturn(2);
		when(keyHolder.getKey()).thenReturn(2L);
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.argThat(new ArgumentMatcher<DBDataSourceMapper>(){

			@Override
			public boolean matches(Object argument) {
				return argument.getClass().equals(DBDataSourceMapper.class);
			}
			
		})))
		.thenReturn(Lists.<DBDataSource>newArrayList(new DBDataSource()));
		int r1=groupService.addRightsToGroup(dbRightGroups, uttestgroup1);
		Assert.assertTrue(r1==1);
		
		groupService.addRightToGroup(uttestgroup1, SysPermission.ADD_DASHBOARD.toString(), PermissionConst.RIGHT_TYPE_SYS);
		groupService.addRightToGroup(uttestgroup1, SysPermission.ADD_DATASOURCE.toString(), PermissionConst.RIGHT_TYPE_SYS);
		groupService.addRightToGroup(uttestgroup1, SysPermission.ADD_GROUP.toString(), PermissionConst.RIGHT_TYPE_SYS);
		groupService.addRightToGroup(uttestgroup1, SysPermission.ADD_MENU.toString(), PermissionConst.RIGHT_TYPE_SYS);
		groupService.addRightToGroup(uttestgroup1, SysPermission.SYS_MANAGE_DASHBOARD.toString(), PermissionConst.RIGHT_TYPE_SYS);
		groupService.addRightToGroup(uttestgroup1, SysPermission.SYS_MANAGE_DATASOURCE.toString(), PermissionConst.RIGHT_TYPE_SYS);
		groupService.addRightToGroup(uttestgroup1, SysPermission.SYS_MANAGE_GROUP.toString(), PermissionConst.RIGHT_TYPE_SYS);
		groupService.addRightToGroup(uttestgroup1, SysPermission.SYS_VIEW_DASHBOARD.toString(), PermissionConst.RIGHT_TYPE_SYS);
		groupService.addRightToGroup(uttestgroup1, SysPermission.SYS_VIEW_DATASOURCE.toString(), PermissionConst.RIGHT_TYPE_SYS);

		
		
		
		
		List<String> userNames=Lists.newArrayList("qxing","xinxu1","ken");	
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBUserGroupMapper.class)))
		.thenReturn(Lists.<DBUserGroup>newArrayList());
		when(db.insert(Mockito.anyString(), Matchers.anyMap(), Matchers.any(GeneratedKeyHolder.class)))
		.thenReturn(1);
		when(keyHolder.getKey()).thenReturn(3L).thenReturn(4L).thenReturn(5L).thenReturn(6L);
		
		
		int r2=groupService.addUsersToGroup(uttestgroup1, userNames);
		Assert.assertTrue(r2==3);
		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.newArrayList(group1));
		List<DBGroup> managedgroup=groupService.getAllUserManagedGroups();
		Assert.assertEquals(Lists.newArrayList(group1), managedgroup);
		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestgroup1)), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.newArrayList(group1));
		DBUserGroup ug1=new DBUserGroup();
		ug1.setGroupName(uttestgroup1);
		ug1.setUserName("qxing");
		DBUserGroup ug2=new DBUserGroup();
		ug2.setGroupName(uttestgroup1);
		ug2.setUserName("xinxu1");
		DBUserGroup ug3=new DBUserGroup();
		ug3.setGroupName(uttestgroup1);
		ug3.setUserName("ken");
		
		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBUserGroupMapper.class)))
		.thenReturn(Lists.newArrayList(ug1,ug2,ug3));
		List<String> users=groupService.getAllUsersInGroup(uttestgroup1);
		Assert.assertEquals(Lists.newArrayList("qxing","xinxu1","ken"), users);
		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBUserGroupMapper.class)))
		.thenReturn(Lists.newArrayList(ug1));

		DBGroup pub=new DBGroup();
		pub.setName("public");
		pub.setOwner("admin");
		when(db.query(Matchers.anyString(),Matchers.any(MapSqlParameterSource.class), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.newArrayList(group1,pub));
		List<DBGroup> groups=groupService.getAllGroupsUserIn("qxing");
		Assert.assertEquals(Lists.newArrayList(group1,pub), groups);
		
		when(db.query(Matchers.anyString(),Matchers.any(MapSqlParameterSource.class), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.newArrayList(group1));
		List<DBGroup> groups2=groupService.getGroupsByNames(Lists.newArrayList(uttestgroup1));
		Assert.assertEquals(Lists.newArrayList(group1), groups2);
		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.newArrayList(group1));
		List<String> groupNames=groupService.getGroupsByOwner(uttestuser);
		Assert.assertEquals(Lists.newArrayList(uttestgroup1), groupNames);
		
		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBRightGroupMapper.class)))
		.thenReturn(dbRightGroups);
		List<String> rights=groupService.getRightNamesByGroupName(uttestgroup1);
		Assert.assertEquals(Lists.newArrayList(rightName), rights);
		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBRightGroupMapper.class)))
		.thenReturn(dbRightGroups);
		List<DBRightGroup> g1rights=groupService.getRightsByGroupName(uttestgroup1);
		Assert.assertEquals(dbRightGroups, g1rights);
		
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("name",uttestgroup1))))
		.thenReturn(1);
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("name",uttestgroup2))))
		.thenReturn(1);
		int rows=groupService.deleteGroups(Lists.newArrayList(uttestgroup1,uttestgroup2));
		Assert.assertEquals(2, rows);
		
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("rightname","datasource1_MANAGE","groupname",uttestgroup1))))
		.thenReturn(1);
		int result1=groupService.removeRightsFromGroup(uttestgroup1, dbRightGroups);
		Assert.assertEquals(1, result1);
		
		group1.setId(2L);
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", uttestgroup1)), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.<DBGroup>newArrayList(group1));
		when(db.update(Mockito.anyString(), Matchers.eq(ImmutableMap.of("id",2L,"name",uttestgroup1,"displayname","Update DispalyName"))))
		.thenReturn(1);
		int result2=groupService.updateGroup(uttestgroup1, "Update DispalyName");
		Assert.assertEquals(1, result2);
		
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("rightname","datasource1_MANAGE","groupname",uttestgroup1))))
		.thenReturn(1);
		int result3=groupService.removeRightFromGroup(uttestgroup1, "datasource1_MANAGE");
		Assert.assertEquals(1, result3);
		
		int result4=groupService.removeRightNamesFromGroup(uttestgroup1, Lists.newArrayList("datasource1_MANAGE"));
		Assert.assertEquals(1, result4);
		
		Set<String> result5=groupService.getAllGroupsForDataSource("datasource1", Lists.newArrayList(group1), "datasource1_MANAGE");
		Assert.assertEquals(Sets.newHashSet(uttestgroup1), result5);
		
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("username",uttestuser,"groupname",uttestgroup1))))
		.thenReturn(1);
		int result6=groupService.removeUsersFromGroup(uttestgroup1, Lists.newArrayList(uttestuser));
		Assert.assertEquals(1,result6);
		
		
		
		Assert.assertTrue(groupService.isValidRight(SysPermission.ADD_DASHBOARD.toString(), PermissionConst.RIGHT_TYPE_SYS));
		Assert.assertTrue(!groupService.isValidRight("ABC", PermissionConst.RIGHT_TYPE_SYS));
		String datasourceName="datasource1_1";
		DBDataSource datasource=new DBDataSource();
		datasource.setName(datasourceName);
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))
		.thenReturn(Lists.newArrayList(datasource));
		Assert.assertTrue(groupService.isValidRight(String.format(PermissionConst.VIEW_RIGHT_TEMPLATE,datasourceName), PermissionConst.RIGHT_TYPE_DATA));
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))
		.thenReturn(Lists.newArrayList());
		Assert.assertTrue(!groupService.isValidRight("ABC", PermissionConst.RIGHT_TYPE_DATA));
		Assert.assertTrue(!groupService.isValidRight("ABC_VIEW", PermissionConst.RIGHT_TYPE_DATA));
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))
		.thenReturn(Lists.newArrayList(datasource));
		Assert.assertTrue(groupService.isValidRight(String.format(PermissionConst.MANAGE_RIGHT_TEMPLATE,datasourceName), PermissionConst.RIGHT_TYPE_DATA));
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDataSourceMapper.class)))
		.thenReturn(Lists.newArrayList());
		Assert.assertTrue(!groupService.isValidRight("ABC", PermissionConst.RIGHT_TYPE_DATA));
		Assert.assertTrue(!groupService.isValidRight("ABC_MANAGE", PermissionConst.RIGHT_TYPE_DATA));
		
		
		String dashboardName="dashboard_1";
		DBDataSource dashboard=new DBDataSource();
		dashboard.setName(dashboardName);
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.newArrayList(dashboard));
		Assert.assertTrue(groupService.isValidRight(String.format(PermissionConst.VIEW_RIGHT_TEMPLATE,dashboardName), PermissionConst.RIGHT_TYPE_DASHBOARD));
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.newArrayList());
		Assert.assertTrue(!groupService.isValidRight("ABC", PermissionConst.RIGHT_TYPE_DASHBOARD));
		Assert.assertTrue(!groupService.isValidRight("ABC_VIEW", PermissionConst.RIGHT_TYPE_DASHBOARD));
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.newArrayList(dashboard));
		Assert.assertTrue(groupService.isValidRight(String.format(PermissionConst.MANAGE_RIGHT_TEMPLATE,dashboardName), PermissionConst.RIGHT_TYPE_DASHBOARD));
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBDashboardMapper.class)))
		.thenReturn(Lists.newArrayList());
		Assert.assertTrue(!groupService.isValidRight("ABC", PermissionConst.RIGHT_TYPE_DASHBOARD));
		Assert.assertTrue(!groupService.isValidRight("ABC_MANAGE", PermissionConst.RIGHT_TYPE_DASHBOARD));
		
		String groupName="group_1";
		DBGroup group=new DBGroup();
		group.setName(groupName);
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.newArrayList(group));
		Assert.assertTrue(groupService.isValidRight(String.format(PermissionConst.VIEW_RIGHT_TEMPLATE,groupName), PermissionConst.RIGHT_TYPE_GROUP));
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.newArrayList());
		Assert.assertTrue(!groupService.isValidRight("ABC", PermissionConst.RIGHT_TYPE_GROUP));
		Assert.assertTrue(!groupService.isValidRight("ABC_VIEW", PermissionConst.RIGHT_TYPE_GROUP));
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.newArrayList(group));
		Assert.assertTrue(groupService.isValidRight(String.format(PermissionConst.MANAGE_RIGHT_TEMPLATE,groupName), PermissionConst.RIGHT_TYPE_GROUP));
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBGroupMapper.class)))
		.thenReturn(Lists.newArrayList());
		Assert.assertTrue(!groupService.isValidRight("ABC", PermissionConst.RIGHT_TYPE_GROUP));
		Assert.assertTrue(!groupService.isValidRight("ABC_MANAGE", PermissionConst.RIGHT_TYPE_GROUP));
    }
    

    
}
