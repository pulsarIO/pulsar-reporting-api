/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;

import com.google.common.collect.ImmutableSet;

/**
 * 
 *@author qxing
 * 
 **/
public class PermissionConst {
	//thirdpartyauthentication
	public static final String THIRD_PARTY_AUTHENTICATION_PASSWORD="d98f3a9f13cf4b0485b89c6462242a31";
	public static final String PUBLICGROUP="public";
	public static final String ADMINGROUP="admin";
	public static final String MANAGE_RIGHT_TEMPLATE="%s_MANAGE";
	public static final String VIEW_RIGHT_TEMPLATE="%s_VIEW";
	public static final String DATA_TABLE_RIGHT_TEMPLATE="%s_%s";
	public static final String RESOURCE_NAME_TEMPLATGE="%s_%d";
	public static final int RIGHT_TYPE_SYS=0;
	public static final int RIGHT_TYPE_DATA=1;
	public static final int RIGHT_TYPE_DASHBOARD=2;
	public static final int RIGHT_TYPE_MENU=3;
	public static final int RIGHT_TYPE_GROUP=4;
	public static final ImmutableSet<String> reservedGroup=ImmutableSet.of(ADMINGROUP);
	
	public static final ImmutableSet<String> sysRights=ImmutableSet.of(
			SysPermission.ADD_DASHBOARD.toString(), 
			SysPermission.ADD_DATASOURCE.toString(),
			SysPermission.ADD_GROUP.toString(),
			SysPermission.SYS_MANAGE_DASHBOARD.toString(),
			SysPermission.SYS_MANAGE_DATASOURCE.toString(),
			SysPermission.SYS_MANAGE_GROUP.toString(),
			SysPermission.SYS_VIEW_DASHBOARD.toString(),
			SysPermission.SYS_VIEW_DATASOURCE.toString(),
			SysPermission.SYS_VIEW_GROUP.toString());
	
	public static boolean isReservedGroup(String groupName){
		return reservedGroup.contains(groupName);
	}
	public static boolean isSysPermission(String rightName){
		return sysRights.contains(rightName);
	}
}
