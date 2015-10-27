/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.auth;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ebay.pulsar.analytics.auth.impl.AuthenticationImpl;

public class AuthModelsTest {
	@Test
	public void testUserInfo(){
		UserInfo userinfo=new UserInfo();
		userinfo.setEnableAuthentication(true);
		userinfo.setEnableAuthorization(true);
		userinfo.setEnabled(true);
		AuthenticationImpl model=new AuthenticationImpl();
		model.authenticationEnabled();
		model.checkSession("test");
		AuthModels.authentication();
		Map<String, Object> ext=new HashMap<String, Object>();
		ext.put("test","test");
		userinfo.setExt(ext);
		userinfo.setLoginSuccess(true);
		userinfo.setToken("test");
		userinfo.setUserName("test");
		assertTrue(userinfo.getExt().equals(ext));
		assertTrue(userinfo.getToken().equals("test"));
		assertTrue(userinfo.getUserName().equals("test"));
		assertTrue(userinfo.isEnableAuthentication());
		assertTrue(userinfo.isEnableAuthorization());
		assertTrue(userinfo.isLoginSuccess());
		assertTrue(userinfo.isEnabled());
	}
	
}
