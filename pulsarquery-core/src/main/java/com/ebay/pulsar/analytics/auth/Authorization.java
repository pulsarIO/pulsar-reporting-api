/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.auth;
/**
 *@author qxing
 * 
 **/
public interface Authorization {
	
	public boolean canAccessData(String user,final String dataSource,final String table);
	/**
	 * check whether the user has some permission
	 * 
	 * throw NoPermissionException if user don't have this permission.
	 * 
	 * @param user
	 * @param permission
	 * @return true if user has this permission.
	 */
	public boolean hasPermission(String user, final String permission);
	/**
	 * check whether authorization model enabled.
	 * 
	 * @return true if enabled. false is not.
	 */
	public boolean authorizationEnabled();
}
