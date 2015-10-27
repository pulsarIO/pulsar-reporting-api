/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.auth;

/**
 * Implement this to enable login.
 * 
 * after authenticate user with password and optional ext, if pass, return an UserInfo to client
 * which contains essential information for UI part. e.g. user name, token, etc.
 * 
 *@author qxing
 * 
 **/
public interface Authentication {
		/**
		 *  Implement login logical with provided userName, password and an optional ext could be anything.
		 *  
		 *  return UserInfo contains login success or not, and some server generated data as well as other user informations.
		 *  
		 *  No exception throws for login failed.
		 * 
		 * @param userName
		 * @param password
		 * @param ext
		 * @return
		 */
		public UserInfo login(String userName, String password) throws Exception;
		/**
		 * check user session.
		 * 
		 * return true if session is valid and not expired.
		 * throw SessionExpiredException for session expired.
		 * throw InvalidSessionException for invalid session.
		 * 
		 * @param userName
		 * @return
		 */
		public boolean checkSession(String userName);
		/**
		 * check whether the authentication model is enabled.
		 * @return true is enabled and false is not.
		 */
		public boolean authenticationEnabled();
}
