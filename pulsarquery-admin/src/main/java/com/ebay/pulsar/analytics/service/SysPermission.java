/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;
/**
 *@author qxing
 *
 *SYS_ADMIN will have all the permissions.
 *
 * 
 **/
public enum SysPermission {
	/**
	 * Datasource permissions,
	 * 
	 * Default the creator will have all the permissions for operation of this data source.
	 * DELETE_DATASOURCE,UPDATE_DATASOURCE,
	 * 
	 */
	ADD_DATASOURCE,//default the create will have all the permission for the adding datasource.
	ADD_GROUP,
	ADD_DASHBOARD,
	ADD_MENU,
	SYS_MANAGE_DATASOURCE,
	SYS_VIEW_DATASOURCE,
	SYS_MANAGE_DASHBOARD,
	SYS_VIEW_DASHBOARD,
	SYS_MANAGE_GROUP,
	SYS_VIEW_GROUP
	
}
