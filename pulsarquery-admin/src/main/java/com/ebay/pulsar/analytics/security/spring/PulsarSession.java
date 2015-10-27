/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.security.spring;

import java.util.Set;

import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.ebay.pulsar.analytics.dao.model.DBUser;

/**
 * 
 *@author qxing
 * 
 **/
public class PulsarSession {
	DBUser user;
	Set<SimpleGrantedAuthority> authorities;
	public PulsarSession(DBUser user,Set<SimpleGrantedAuthority> authorities){
		this.user=user;
		this.authorities=authorities;
	}
	public DBUser getUser() {
		return user;
	}
	public Set<SimpleGrantedAuthority> getAuthorities() {
		return authorities;
	}

}
