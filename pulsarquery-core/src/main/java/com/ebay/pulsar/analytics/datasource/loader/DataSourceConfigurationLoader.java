/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource.loader;

/**
 * 
 * @author mingmwang
 *
 */
public interface DataSourceConfigurationLoader {
	public static final String DATASOURCE_TYPE = "datasourcetype";
	public static final String ENDPOINTS = "endpoints";
	public static final String DATASOURCE_NAME = "datasourcename";
	public static final String PULSAR_DATASOURCE = "pulsarholap";
	
	public void load();
}
