/*******************************************************************************
*  Copyright ? 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.access.prepost.PreFilter;
import org.springframework.stereotype.Service;

import com.ebay.pulsar.analytics.dao.model.DBDashboard;
import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.model.DBGroup;
import com.ebay.pulsar.analytics.dao.model.DBRightGroup;
import com.ebay.pulsar.analytics.dao.model.DBUser;
import com.ebay.pulsar.analytics.dao.model.DBUserGroup;
import com.ebay.pulsar.analytics.dao.service.DBDashboardService;
import com.ebay.pulsar.analytics.dao.service.DBDataSourceService;
import com.ebay.pulsar.analytics.dao.service.DBGroupService;
import com.ebay.pulsar.analytics.dao.service.DBRightGroupService;
import com.ebay.pulsar.analytics.dao.service.DBUserGroupService;
import com.ebay.pulsar.analytics.dao.service.DBUserService;
import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.security.spring.PermissionControlCache;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author qxing
 * 
 **/
@Service
public class GroupService {
	private DBGroupService groupService;
	private DBRightGroupService rightGroupService;
	private DBUserGroupService userGroupService;
	private DBUserService userService;
	private DBDashboardService dashboardService;
	private DBDataSourceService dataSourceService;

	public GroupService() {
		this.groupService = new DBGroupService();
		this.rightGroupService = new DBRightGroupService();
		this.userGroupService = new DBUserGroupService();
		this.userService = new DBUserService();
		this.dashboardService=new DBDashboardService();
		this.dataSourceService=new DBDataSourceService();
	}

	@PreAuthorize("hasAuthority('ADD_GROUP')")
	public long addGroup(DBGroup group) {
		checkNotNull(group);
		checkArgument(group.getName() != null && !"".equals(group.getName()),
				"group name could not be empty.");
		checkArgument(!StringUtils.isEmpty(group.getOwner()),
				"group owner must be specified.");
		if (group.getDisplayName() == null)
			group.setDisplayName(group.getName());
		if (group.getCreateTime() == null)
			group.setCreateTime(new Date());
		group.setName(group.getName());
		DBGroup condition = new DBGroup();
		condition.setName(group.getName());
		List<DBGroup> list = groupService.get(condition);
		checkState(list == null || list.size() == 0,
				"[%s] group name already exists.", group.getName());
		long id = groupService.inser(group);
		checkState(id > 0, "insert group to db failed.groupName=%s,owner=",
				group.getName(), group.getOwner());
		DBGroup update =new DBGroup();
		update.setId(id);
		update.setName(String.format(PermissionConst.RESOURCE_NAME_TEMPLATGE,group.getName(), id));
		int row=groupService.updateById(update);
		if(row>0){
			group.setId(id);
			group.setName(String.format(PermissionConst.RESOURCE_NAME_TEMPLATGE,group.getName(), id));
		}
		PermissionControlCache.getInstance().expireSessions(group.getOwner());
		return id;
	}

	@PreAuthorize("hasAuthority(#groupName+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public int deleteGroup(String groupName) {
		// Delete User-Group-Mapping entities for this group
		// Delete Right-Group-Mapping entities for this group
		// Delete Group entity for this group.

		checkNotNull(groupName);
		checkArgument(!PermissionConst.isReservedGroup(groupName),"Can NOT delete "+groupName+" group");
		checkArgument(!PermissionConst.PUBLICGROUP.equalsIgnoreCase(groupName),"Can NOT delete "+PermissionConst.PUBLICGROUP+" group");

		DBUserGroup userGroupCondition = new DBUserGroup();
		userGroupCondition.setGroupName(groupName);
		this.userGroupService.deleteBatch(userGroupCondition);
		DBRightGroup rightGroupCondition = new DBRightGroup();
		rightGroupCondition.setGroupName(groupName);
		this.rightGroupService.deleteBatch(rightGroupCondition);
		rightGroupService.deleteRightsFromGroupByPrefix(groupName + "_");
		DBGroup groupCondition = new DBGroup();
		groupCondition.setName(groupName);
		int i= this.groupService.deleteBatch(groupCondition);
		PermissionControlCache.getInstance().expireSessionsAll();
		return i;
	}

	@PreFilter("hasAuthority(filterObject+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public int deleteGroups(List<String> groupNames) {
		int rows = 0;
		for (String groupName : groupNames) {
			try{
			rows += deleteGroup(groupName);
			}catch(Exception e){
				
			}
		}
		PermissionControlCache.getInstance().expireSessionsAll();
		return rows;
	}

	@PreAuthorize("hasAuthority(#groupName+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public int updateGroup(String groupName, String displayname) {

		checkNotNull(groupName);
		checkNotNull(displayname);
		checkArgument(!PermissionConst.isReservedGroup(groupName),"Can NOT update "+groupName+" group");
		checkArgument(!PermissionConst.PUBLICGROUP.equalsIgnoreCase(groupName),"Can NOT update "+PermissionConst.PUBLICGROUP+" group");
		
		DBGroup condition = new DBGroup();
		condition.setName(groupName);
		List<DBGroup> list = groupService.get(condition);
		checkState(list.get(0) != null, "[%s] group is not exists.", groupName);
		condition.setId(list.get(0).getId());
		condition.setDisplayName(displayname);
		int response = groupService.updateById(condition);

		return response;
	}

	@PreAuthorize("hasAnyAuthority('SYS_MANAGE_GROUP', #groupName+'_MANAGE')")
	public int addUserToGroup(String groupName, String userName) {
		DBUser user = new DBUser();
		user.setName(userName);
		List<DBUser> listUser = userService.get(user);
		if (listUser == null || listUser.size() == 0) {
			user.setCreateTime(new Date());
			user.setComment(String.format("First add user to group %s",
					groupName));
			userService.inser(user);
		}

		DBUserGroup ug = new DBUserGroup();
		ug.setGroupName(groupName);
		ug.setUserName(userName);
		List<DBUserGroup> ugList = userGroupService.get(ug);
		if (ugList != null && ugList.size() > 0)
			return 1;
		ug.setCreateTime(new Date());
		long id = userGroupService.inser(ug);
		checkState(id > 0, "Add user[%s] to group [%s] failed.", userName,
				groupName);
		PermissionControlCache.getInstance().expireSessions(userName);
		return 1;
	}

	@PreAuthorize("hasAuthority(#groupName+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public int addUsersToGroup(String groupName, List<String> userNames) {
		if (userNames == null || userNames.size() <= 0)
			return 0;
		int rows=0;
		for (String user : userNames) {
			try{
				rows+=addUserToGroup(groupName, user);
			}catch(Exception e){
			}
		
		}
		PermissionControlCache.getInstance().expireSessions(userNames);
		return rows;
	}

	@PreAuthorize("hasAuthority(#groupName+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public int removeUserFromGroup(String groupName, String userName) {
		DBUserGroup ug = new DBUserGroup();
		ug.setGroupName(groupName);
		ug.setUserName(userName);
		int i= userGroupService.deleteBatch(ug);
		PermissionControlCache.getInstance().expireSessions(userName);
		return i;
	}

	@PreAuthorize("hasAuthority(#groupName+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public int removeUsersFromGroup(String groupName, List<String> userNames) {
		if (userNames == null || userNames.size() <= 0)
			return 0;
		int rows = 0;
		for (String user : userNames) {
			try{
				rows += removeUserFromGroup(groupName, user);
			}catch(Exception e){
			}
		}
		PermissionControlCache.getInstance().expireSessions(userNames);
		return rows;
	}

	private int addRight2Group(String groupName,String rightName,int rightType){
		DBRightGroup rightGroup = new DBRightGroup();
		rightGroup.setGroupName(groupName);
		rightGroup.setRightName(rightName);
		rightGroup.setRightType(rightType);
		List<DBRightGroup> list = rightGroupService.get(rightGroup);
		if (list != null && list.size() > 0)
			return 1;
		rightGroup.setCreateTime(new Date());
		Long id = rightGroupService.inser(rightGroup);
		rightGroup.setId(id);
		checkState(id > 0, "add right [%s] to group [%s] failed.", rightName,
				groupName);
		PermissionControlCache.getInstance().expireSessionsAll();
		return 1;
	}
	
	//@PreAuthorize("hasAnyAuthority('SYS_MANAGE_GROUP', #groupName+'_MANAGE') and (hasAnyAuthority('SYS_MANAGE_DATASOURCE','SYS_MANAGE_DASHBOARD',#rightName))")
	@PreAuthorize("hasAnyAuthority('SYS_MANAGE_GROUP', #groupName+'_MANAGE') and (hasAuthority(#rightName) or (#rightName.endsWith('_MANAGE') and (((#rightType==1) and hasAuthority('SYS_MANAGE_DATASOURCE')) or ((#rightType==2) and hasAuthority('SYS_MANAGE_DASHBOARD')) or ((#rightType==4) and hasAuthority('SYS_MANAGE_GROUP')))) or (#rightName.endsWith('_VIEW') and (((#rightType==1) and hasAnyAuthority('SYS_MANAGE_DATASOURCE','SYS_VIEW_DATASOURCE')) or ((#rightType==2) and hasAnyAuthority('SYS_MANAGE_DASHBOARD','SYS_VIEW_DASHBOARD')) or ((#rightType==4) and hasAnyAuthority('SYS_MANAGE_GROUP','SYS_VIEW_GROUP')))))")
	public int addRightToGroup(String groupName, String rightName,
			int rightType) {
		
		checkArgument(groupName != null && !"".equals(groupName),
				"group name could not be empty.");
		checkArgument(rightName != null && !"".equals(rightName),
				"right name could not be empty.");
		checkArgument(rightType == PermissionConst.RIGHT_TYPE_SYS
				|| rightType == PermissionConst.RIGHT_TYPE_DATA
				|| rightType == PermissionConst.RIGHT_TYPE_DASHBOARD
				|| rightType == PermissionConst.RIGHT_TYPE_GROUP,
				"Invalid right type[%d]", rightType);
		checkArgument(isValidRight(rightName, rightType),"Invalid right ["+rightName+"] with rightType="+rightType);
		checkArgument(!PermissionConst.isReservedGroup(groupName),"Can NOT add right to "+groupName+" group");
		return this.addRight2Group(groupName, rightName, rightType);
	}

	//@PreFilter(value = "hasAnyAuthority(filterObject.rightName,'SYS_MANAGE_DATASOURCE','SYS_MANAGE_DASHBOARD') and hasAnyAuthority('SYS_MANAGE_GROUP', #groupName+'_MANAGE')", filterTarget = "dbRightGroups")
	@PreFilter(value="hasAnyAuthority('SYS_MANAGE_GROUP', #groupName+'_MANAGE') and (hasAuthority(filterObject.rightName) or (filterObject.rightName.endsWith('_MANAGE') and (((filterObject.rightType==1) and hasAuthority('SYS_MANAGE_DATASOURCE')) or ((filterObject.rightType==2) and hasAuthority('SYS_MANAGE_DASHBOARD')) or ((filterObject.rightType==4) and hasAuthority('SYS_MANAGE_GROUP')))) or (filterObject.rightName.endsWith('_VIEW') and (((filterObject.rightType==1) and hasAnyAuthority('SYS_MANAGE_DATASOURCE','SYS_VIEW_DATASOURCE')) or ((filterObject.rightType==2) and hasAnyAuthority('SYS_MANAGE_DASHBOARD','SYS_VIEW_DASHBOARD')) or ((filterObject.rightType==4) and hasAnyAuthority('SYS_MANAGE_GROUP','SYS_VIEW_GROUP')))))", filterTarget = "dbRightGroups")
	public int addRightsToGroup(List<DBRightGroup> dbRightGroups,
			String groupName) {
		if (dbRightGroups == null || dbRightGroups.size() <= 0)
			return 0;
		int rows=0;
		for (DBRightGroup dbRightGroup : dbRightGroups) {
			try{
			rows+=addRightToGroup(groupName, dbRightGroup.getRightName(),
					dbRightGroup.getRightType());
			}catch(Exception e){
				
			}
		}
		return rows;
	}

	@PreAuthorize("hasAnyAuthority('SYS_MANAGE_GROUP', #groupName+'_MANAGE') and hasAnyAuthority('SYS_MANAGE_DATASOURCE','SYS_MANAGE_DASHBOARD',#rightName)")
	public int removeRightFromGroup(String groupName, String rightName) {
		checkArgument(groupName != null && !"".equals(groupName),
				"group name could not be empty.");
		checkArgument(rightName != null && !"".equals(rightName),
				"right name could not be empty.");
		checkArgument(!PermissionConst.isReservedGroup(groupName),"Can NOT remove right from "+PermissionConst.ADMINGROUP+" group");
		DBRightGroup rightGroup = new DBRightGroup();
		rightGroup.setGroupName(groupName);
		rightGroup.setRightName(rightName);
		List<DBRightGroup> list = rightGroupService.get(rightGroup);
		if (list == null || list.size() == 0)
			return 0;
		int i= rightGroupService.deleteBatch(rightGroup);
		PermissionControlCache.getInstance().expireSessionsAll();
		return i;
	}

	@PreFilter(value = "hasAnyAuthority(filterObject.rightName,'SYS_MANAGE_DATASOURCE','SYS_MANAGE_DASHBOARD') and hasAnyAuthority('SYS_MANAGE_GROUP', #groupName+'_MANAGE')", filterTarget = "dbRightGroups")
	public int removeRightsFromGroup(String groupName,
			List<DBRightGroup> dbRightGroups) {
		if (dbRightGroups == null || dbRightGroups.size() <= 0)
			return 0;
		int rows = 0;
		for (DBRightGroup dbRightGroup : dbRightGroups) {
			try{
			rows += removeRightFromGroup(groupName, dbRightGroup.getRightName());
			}catch(Exception e){
				
			}
		}
		return rows;
	}
	
	@PreFilter(value = "hasAnyAuthority(filterObject,'SYS_MANAGE_DATASOURCE','SYS_MANAGE_DASHBOARD') and hasAnyAuthority('SYS_MANAGE_GROUP', #groupName+'_MANAGE')", filterTarget = "rightNames")
	public int removeRightNamesFromGroup(String groupName,
			List<String> rightNames) {
		if (rightNames == null || rightNames.size() <= 0)
			return 0;
		int rows = 0;
		for (String rightName : rightNames) {
			try{
			rows += removeRightFromGroup(groupName, rightName);
			}catch(Exception e){
				
			}
		}
		return rows;
	}

	public List<String> getGroupsByOwner(String user) {
		checkArgument(user != null && !"".equals(user),
				"user name could not be empty.");
		DBGroup condition = new DBGroup();
		condition.setOwner(user);
		List<DBGroup> list = groupService.get(condition);
		return Lists.transform(list, new Function<DBGroup, String>() {
			@Override
			public String apply(DBGroup input) {
				return input.getName();
			}
		});
	}

	@PreFilter("hasAuthority(filterObject+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public List<DBGroup> getGroupsByNames(List<String> names) {
		return groupService.getAllByColumnIn("name", names, -1);
	}

	@PostFilter("hasAuthority(filterObject+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public List<DBGroup> getAllGroupsUserIn(String user) {
		checkArgument(user != null && !"".equals(user),
				"user name could not be empty.");
		DBUserGroup condition = new DBUserGroup();
		condition.setUserName(user);
		List<String> groupNames = FluentIterable
				.from(userGroupService.get(condition))
				.transform(new Function<DBUserGroup, String>() {
					@Override
					public String apply(DBUserGroup input) {
						return input.getGroupName();
					}
				}).append(PermissionConst.PUBLICGROUP).toList();// Add "Public"
																// Group for all
																// users
		return getGroupsByNames(groupNames);
	}

	@PreAuthorize("hasAuthority(#groupName+'_MANAGE') or hasAuthority(#groupName+'_VIEW') or hasAuthority('SYS_MANAGE_GROUP')")
	public List<String> getAllUsersInGroup(String groupName) {
		checkArgument(groupName != null && !"".equals(groupName),
				"groupName name could not be empty.");
		DBUserGroup condition = new DBUserGroup();
		condition.setGroupName(groupName);
		List<String> userNames = FluentIterable
				.from(userGroupService.get(condition))
				.transform(new Function<DBUserGroup, String>() {
					@Override
					public String apply(DBUserGroup input) {
						return input.getUserName();
					}
				}).toList();
		return userNames;
//		return FluentIterable
//				.from(userService.getAllByColumnIn("name", userNames, -1))
//				.transform(new Function<DBUser, String>() {
//					public String apply(DBUser input) {
//						return input.getName();
//					}
//				}).toList();
	}

	@PreAuthorize("hasAuthority(#groupName+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public List<DBRightGroup> getRightsByGroupName(String groupName) {
		checkArgument(groupName != null && !"".equals(groupName),
				"groupName name could not be empty.");
		DBRightGroup condition = new DBRightGroup();
		condition.setGroupName(groupName);
		return rightGroupService.get(condition);

	}

	@PostFilter("hasAuthority(filterObject.name+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public List<DBGroup> getAllUserManagedGroups() {
		return groupService.getAll();
	}
	@PostFilter("hasAuthority(filterObject.name+'_MANAGE') or hasAuthority(filterObject.name+'_VIEW') or hasAuthority('SYS_MANAGE_GROUP') or hasAuthority('SYS_VIEW_GROUP')")
	public List<DBGroup> getAllUserViewedGroups() {
		return groupService.getAll();
		
	}
	@PostFilter("hasAuthority(filterObject+'_MANAGE') or hasAuthority('SYS_MANAGE_GROUP')")
	public Set<String> getAllGroupsForDataSource(String datasourceName,
			List<DBGroup> groups, String permission) {
		final String permissionType = permission;
		Set<String> groupName = FluentIterable.from(groups)
				.filter(new Predicate<DBGroup>() {
					@Override
					public boolean apply(DBGroup input) {
						return getRightNamesByGroupName(input.getName())
								.contains(permissionType);
					}
				}).transform(new Function<DBGroup, String>() {
					@Override
					public String apply(DBGroup input) {
						return input.getName();
					}
				}).toSet();

		return Sets.newHashSet(groupName);
	}

	public List<String> getRightNamesByGroupName(String groupName) {
		return FluentIterable.from(getRightsByGroupName(groupName))
				.transform(new Function<DBRightGroup, String>() {
					@Override
					public String apply(DBRightGroup input) {
						return input.getRightName();
					}
				}).toList();

	}
	public boolean isValidRight(String rightName, int rightType){
		if(rightType == PermissionConst.RIGHT_TYPE_SYS){
			try{
				SysPermission permission=Enum.valueOf(SysPermission.class, rightName);
				return permission!=null;
			}catch(Exception e){
			}
		}else{
			String dname=null;
			if(rightName.endsWith("_MANAGE")){
				dname=rightName.substring(0, rightName.lastIndexOf("_MANAGE"));
			}else if(rightName.endsWith("_VIEW")){
				dname=rightName.substring(0, rightName.lastIndexOf("_VIEW"));
			}else{
				return false;
			}
			if(rightType == PermissionConst.RIGHT_TYPE_DATA){
				if(this.isStaticDataSource(dname)) return true;
				DBDataSource condition=new DBDataSource();
				condition.setName(dname);
				List<DBDataSource> list=dataSourceService.get(condition);
				return list!=null && list.size()>0;
			}else if(rightType == PermissionConst.RIGHT_TYPE_DASHBOARD){
				DBDashboard condition=new DBDashboard();
				condition.setName(dname);
				List<DBDashboard> list=dashboardService.get(condition);
				return list!=null && list.size()>0;
			}else if(rightType == PermissionConst.RIGHT_TYPE_GROUP){
				DBGroup condition=new DBGroup();
				condition.setName(dname);
				List<DBGroup> list=groupService.get(condition);
				return list!=null && list.size()>0;
			}
		}
		return false;
	}
	private boolean isStaticDataSource(String name){
		for (Entry<String, DataSourceConfiguration> entry : DataSourceMetaRepo
				.getInstance().getDbConfMap().entrySet()) {
			DataSourceConfiguration conf=entry.getValue();
			if(conf.isRealOnly()){
				if(entry.getKey().equals(name)) return true;
			}
		}
		return false;
	}
}
