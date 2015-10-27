/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.security.spring;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.intercept.aopalliance.MethodSecurityInterceptor;

import com.google.common.base.Preconditions;

/**
 * @author qxing
 * 
 *  Modify <code>MethodSecurityInterceptor</code> property <property>alwaysReauthenticate</property>
 *  set it to true.
 **/
public class GlobalSecuritySetting implements InitializingBean {
	@Autowired
	MethodSecurityInterceptor interceptor;

	@Override
	public void afterPropertiesSet() throws Exception {
		Preconditions.checkNotNull(interceptor);
		if (!interceptor.isAlwaysReauthenticate()) {
			interceptor.setAlwaysReauthenticate(true);
		}
	}
}
