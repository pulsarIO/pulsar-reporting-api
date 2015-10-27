/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.auth;

import com.ebay.pulsar.analytics.config.ConfigurationFactory;
import com.google.common.base.Throwables;

/**
 *@author qxing
 * 
 **/
public class AuthModels {
		public static final String AUTHENTICATION_ENABLE_KEY="pulsar.analytics.authentication.enable";
		public static final String AUTHORIZATION_ENABLE_KEY="pulsar.analytics.authorization.enable";

		public static final String AUTHENTICATION_IMPLEMENTATION_KEY="pulsar.analytics.authentication.impl";
		public static final String AUTHORIZATION_IMPLEMENTATION_KEY="pulsar.analytics.authorization.impl";
		public static Authentication authentication(){
			return AuthenticationHolder.auth;
		}
		public static Authorization authorization(){
			return AuthorizationHolder.auth;
		}
		
		private static <T> T  instance(String clazz, Class<T > parent){
			try{
				Class<?> clz=Class.forName(clazz);
		    	if(parent.isAssignableFrom(clz)){
		    		Object obj=clz.newInstance();
		    		T ret=parent.cast(obj);
		    		return ret;
		    	}
		    	throw new RuntimeException("class["+clazz+"] is not assignable to ["+parent.getName()+"].");
			}catch(Exception e){
				Throwables.propagate(e);
			}
			return null;
		}
		
		private static class AuthenticationHolder{
			private static volatile  Authentication auth;
			static{
				auth=instance(ConfigurationFactory.instance().getString(AUTHENTICATION_IMPLEMENTATION_KEY),
						Authentication.class);
			}
		}
		private static class AuthorizationHolder{
			private static volatile Authorization auth;
			static{
				auth=instance(ConfigurationFactory.instance().getString(AUTHORIZATION_IMPLEMENTATION_KEY),
						Authorization.class);
			}
		}
}
