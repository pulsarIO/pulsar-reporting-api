/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;

import java.util.List;
import java.util.Set;

import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Service;

import com.ebay.pulsar.analytics.dao.model.DBGroup;
import com.ebay.pulsar.analytics.dao.model.DBRightGroup;
import com.ebay.pulsar.analytics.dao.service.DBRightGroupService;
import com.ebay.pulsar.analytics.dao.service.DirectSQLAccessService;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

/**
 * @author qxing
 * 
 **/
@Service
public class UserPermissionControl {

	private GroupService groupService = new GroupService();
	private DBRightGroupService rightGroupService = new DBRightGroupService();
	private DashboardService dashboardService = new DashboardService();
	private DataSourceService datasourceService = new DataSourceService();
	private DirectSQLAccessService directSQLAccessService=new DirectSQLAccessService();

	// Get the rights in the user's Groups, including the groups user created
	public List<DBRightGroup> getAllRightsByUserName(String user) {
		return rightGroupService
				.getAllByColumnIn(
						"groupName",
						FluentIterable
								.from(groupService.getAllGroupsUserIn(user))
								.transform(new Function<DBGroup, String>() {
									public String apply(DBGroup input) {
										return input.getName();
									}
								}).append(groupService.getGroupsByOwner(user))
								.toList(), -1);
	}

	public List<String> getAllRightsForOwner(String userName) {
		List<String> datasourceRights = FluentIterable
				.from(datasourceService.getAllDataSourcesForOwner(userName))
				.transform(new Function<String, String>() {
					public String apply(String input) {
						return String.format(
								PermissionConst.MANAGE_RIGHT_TEMPLATE, input);
					}
				}).toList();
		List<String> dashboardRights = FluentIterable
				.from(dashboardService.getAllDashboardsForOwner(userName))
				.transform(new Function<String, String>() {
					public String apply(String input) {
						return String.format(
								PermissionConst.MANAGE_RIGHT_TEMPLATE, input);
					}
				}).toList();

		return FluentIterable.from(groupService.getGroupsByOwner(userName))
				.transform(new Function<String, String>() {
					public String apply(String input) {
						return String.format(
								PermissionConst.MANAGE_RIGHT_TEMPLATE, input);
					}
				}).append(datasourceRights).append(dashboardRights).toList();
	}

	
	public List<String> getAllRightsForUser(String userName){
		return this.directSQLAccessService.getAllRightsForValidUser(userName);
	}
	
	// All the rights for login user
	public Set<SimpleGrantedAuthority> getAllRightsForValidUser(String userName) {
	    List<String> allrights=getAllRightsForUser(userName);
	  
	    return FluentIterable.from(allrights).filter(new Predicate<String>(){
			@Override
			public boolean apply(String input) {
				if(input.endsWith("_MANAGE")){
					return true;
				}
				return false;
			}
	    }).transform(new Function<String,SimpleGrantedAuthority>(){
			@Override
			public SimpleGrantedAuthority apply(String input) {
				if(input.endsWith("_MANAGE")){
					return new SimpleGrantedAuthority(input.substring(0,input.lastIndexOf("_MANAGE"))+"_VIEW");
				}
				return new SimpleGrantedAuthority(input);
			}
	    }).append(FluentIterable.from(allrights).transform(new Function<String,SimpleGrantedAuthority>(){
			@Override
			public SimpleGrantedAuthority apply(String input) {
				return new SimpleGrantedAuthority(input);
			}
	    })).toSet();

	}



}
