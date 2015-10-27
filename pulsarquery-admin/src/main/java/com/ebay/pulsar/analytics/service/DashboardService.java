/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.access.prepost.PreFilter;
import org.springframework.stereotype.Service;

import com.ebay.pulsar.analytics.dao.model.DBDashboard;
import com.ebay.pulsar.analytics.dao.service.DBDashboardService;
import com.ebay.pulsar.analytics.dao.service.DBRightGroupService;
import com.ebay.pulsar.analytics.security.spring.PermissionControlCache;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Service used to manage dashboard.
 * 
 * @author xinxu1
 * 
 **/

@Service
public class DashboardService {
	private final DBDashboardService dashboardService;
	private final DBRightGroupService rightGroupService;

	public DashboardService() {
		this.dashboardService = new DBDashboardService();
		this.rightGroupService = new DBRightGroupService();
	}

	public String getManagePermission(String dashboardName) {
		return String.format(PermissionConst.MANAGE_RIGHT_TEMPLATE,
				dashboardName);
	}
	
	public String getViewPermission(String dashboardName) {
		return String.format(PermissionConst.VIEW_RIGHT_TEMPLATE,
				dashboardName);
	}
	
	@PreAuthorize("hasAuthority('ADD_DASHBOARD')")
	public long addDashboard(DBDashboard dashboard) {
		checkNotNull(dashboard);
		checkArgument(
				dashboard.getName() != null && !"".equals(dashboard.getName()),
				"dashboard name could not be empty.");
		checkArgument(!StringUtils.isEmpty(dashboard.getOwner()),
				"dashboard owner must be specified.");
		if (dashboard.getDisplayName() == null)
			dashboard.setDisplayName(dashboard.getName());
		if (dashboard.getCreateTime() == null)
			dashboard.setCreateTime(new Date());
		dashboard.setName(dashboard.getName());
		DBDashboard dashboardCondition = new DBDashboard();
		dashboardCondition.setName(dashboard.getName());
		List<DBDashboard> list = dashboardService.get(dashboardCondition);
		checkState(list == null || list.size() == 0,
				"[%s] dashboard name already exists.", dashboard.getName());
		long id = dashboardService.inser(dashboard);
		checkState(id > 0,
				"insert dashboard to db failed.dashboardName=%s,owner=",
				dashboard.getName(), dashboard.getOwner());
		DBDashboard update=new DBDashboard();
		update.setId(id);
		update.setName(String.format(PermissionConst.RESOURCE_NAME_TEMPLATGE,dashboard.getName(), id));
		int row=dashboardService.updateById(update);
		if(row>0){
			dashboard.setId(id);
			dashboard.setName(String.format(PermissionConst.RESOURCE_NAME_TEMPLATGE,dashboard.getName(), id));
		}
		PermissionControlCache.getInstance().expireSessions(dashboard.getOwner());
		return id;
	}
	@PreAuthorize("hasAnyAuthority(#dashboardName+'_MANAGE','SYS_MANAGE_DASHBOARD')")
	public int deleteDashboard(String dashboardName) {

		checkNotNull(dashboardName);

		rightGroupService.deleteRightsFromGroupByPrefix(dashboardName+"_");
		DBDashboard dashboardCondition = new DBDashboard();
		dashboardCondition.setName(dashboardName);
		int i= dashboardService.deleteBatch(dashboardCondition);
		PermissionControlCache.getInstance().expireSessionsAll();
		return i;
	}
	@PreFilter("hasAuthority(filterObject+'_MANAGE') or hasAuthority('SYS_MANAGE_DASHBOARD')")
	public int deleteDashboards(List<String> dashboardNames){
		int rows=0;
		for(String dashboardName: dashboardNames){
			try{
			rows+=deleteDashboard(dashboardName);
			}catch(Exception e){
				
			}
		}
		PermissionControlCache.getInstance().expireSessionsAll();
		return rows;
	}
	@PreAuthorize("hasAuthority(#dashboardName+'_MANAGE') or hasAuthority('SYS_MANAGE_DASHBOARD')")
	public int updateDashboard(String dashboardName, String displayname, String newConfig) {

		checkNotNull(dashboardName);
		DBDashboard dashboardCondition = new DBDashboard();
		dashboardCondition.setName(dashboardName);
		List<DBDashboard> list = dashboardService.get(dashboardCondition);
		checkState(list.get(0) != null, "[%s] dashboard is not exists.",
				dashboardName);
		dashboardCondition.setId(list.get(0).getId());
		if(newConfig!=null){
			dashboardCondition.setConfig(newConfig);
		}
		if(displayname!=null){
			dashboardCondition.setDisplayName(displayname);
		}
		int response = dashboardService.updateById(dashboardCondition);
		return response;
	}

	//@PreAuthorize("hasAuthority(#dashboardName+'_MANAGE') or hasAuthority(#dashboardName+'_VIEW') or hasAuthority('SYS_MANAGE_DASHBOARD') or hasAuthority('SYS_VIEW_DASHBOARD')")
	@PreAuthorize("hasAnyAuthority(#dashboardName+'_MANAGE',#dashboardName+'_VIEW','SYS_MANAGE_DASHBOARD','SYS_VIEW_DASHBOARD')")
	public DBDashboard getDashboardByName(String dashboardName) {

		checkNotNull(dashboardName);
		DBDashboard dashboardCondition = new DBDashboard();
		dashboardCondition.setName(dashboardName);
		List<DBDashboard> list = dashboardService.get(dashboardCondition);
		checkState(list.size() == 1, "[%s] dashboard is not exists.",
				dashboardName);
		return list.get(0);
	}

	public List<String> getAllDashboardsForOwner(String userName) {

		checkNotNull(userName);
		DBDashboard dashboardCondition = new DBDashboard();
		dashboardCondition.setOwner(userName);
		return Lists.transform(dashboardService.get(dashboardCondition), new Function<DBDashboard,String>(){
			@Override
			public String apply(DBDashboard input) {
				return input.getName();
			}
		});
	}
	@PreFilter("hasAuthority(filterObject+'_MANAGE') or hasAuthority(filterObject+'_VIEW') or hasAuthority('SYS_MANAGE_DASHBOARD') or hasAuthority('SYS_VIEW_DASHBOARD')")
	public List<DBDashboard> getDashboardByNames(List<String> names){
		return dashboardService.getAllByColumnIn("name", names, -1);
	}
	@PostFilter("hasAuthority(filterObject.name+'_MANAGE') or hasAuthority(filterObject.name+'_VIEW') or hasAuthority('SYS_MANAGE_DASHBOARD') or hasAuthority('SYS_VIEW_DASHBOARD')")
	public List<DBDashboard> getUserViewedDashboard(){
			return dashboardService.getAll();
	}
	@PostFilter("hasAuthority(filterObject.name+'_MANAGE') or hasAuthority('SYS_MANAGE_DASHBOARD')")
	public List<DBDashboard> getAllUserManagedDashboard(){
		return dashboardService.getAll();
	}
	
}
