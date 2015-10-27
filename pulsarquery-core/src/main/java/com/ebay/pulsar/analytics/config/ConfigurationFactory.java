/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.config;

import com.ebay.pulsar.analytics.auth.AuthModels;

/**
 *@author qxing
 * 
 **/
public class ConfigurationFactory {
	public static Configuration instance(){
		return ConfigurationHolder.conf;
	}
	/**
	 * Please implementation this method to load all the configurations.
	 * 
	 * @return
	 */
	
	private static Configuration loadConfig(){
		return new Configuration(){

			@Override
			public String getString(String key) {
				if(AuthModels.AUTHENTICATION_IMPLEMENTATION_KEY.equals(key)){
					return "com.ebay.pulsar.analytics.auth.impl.AuthenticationImpl";
				}else if(AuthModels.AUTHORIZATION_IMPLEMENTATION_KEY.equals(key)){
					return "com.ebay.pulsar.analytics.auth.impl.AuthorizationImpl";
				}else if(AuthModels.AUTHENTICATION_ENABLE_KEY.equals(key)){
					return "true";
				}else if(AuthModels.AUTHORIZATION_ENABLE_KEY.equals(key)){
					return "true";
				}else{
					return "";
				}
			}
			
		};
	}
	
	private static class ConfigurationHolder{
		private static Configuration conf;
		static{
			conf=loadConfig();
		}
	}
}
