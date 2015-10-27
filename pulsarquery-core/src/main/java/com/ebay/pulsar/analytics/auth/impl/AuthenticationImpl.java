/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.auth.impl;

import com.ebay.pulsar.analytics.auth.Authentication;
import com.ebay.pulsar.analytics.auth.UserInfo;

public class AuthenticationImpl implements Authentication {
	//private UserService userService = new UserService();
	//private UserInfo userInfo = new UserInfo();

//	public String getBASE64(String input) throws Exception {
//		if (input == null)
//			return null;
//		return DatatypeConverter.printBase64Binary(input.getBytes("UTF-8"));
//	}
//
//	public String getMD5(String input) throws Exception {
//		if (input == null)
//			return null;
//
//		StringBuffer sb = new StringBuffer(32);
//		try {
//			MessageDigest md = MessageDigest.getInstance("MD5");
//			byte[] buff = md.digest(input.getBytes("UTF-8"));
//			for (int i = 0; i < buff.length; i++) {
//				sb.append(Integer.toHexString((buff[i] & 0xFF) | 0x100)
//						.toUpperCase().substring(1, 3));
//			}
//		} catch (Exception e) {
//			throw new RuntimeException("Encoding " + input + "failed!");
//		}
//
//		return sb.toString();
//	}

	@Override
	public UserInfo login(String userName, String password) throws Exception {

//		userInfo.setUserName(userName);
//		Map<String, Object> map = new HashMap<String, Object>();
//		map.put("email", userService.getUserByName(userName).getEmail());
//		map.put("image", userService.getUserByName(userName).getImage());
//		userInfo.setExt(map);
//		userInfo.setLoginSuccess(false);
//		if (userService.isValidUser(userName, getMD5(password))) {
//			String authToken = getBASE64(UUID.randomUUID().toString());
//			userInfo.setLoginSuccess(true);
//			userInfo.setToken(authToken);
//		}
//		return userInfo;
		return null;
	}

	@Override
	public boolean checkSession(String userName) {
		return false;
	}

	@Override
	public boolean authenticationEnabled() {
		return true;
	}

}