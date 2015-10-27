/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.exception;

public class DataSourceConfigurationException extends RuntimeException {
	private static final long serialVersionUID = -3775554654653365202L;
	
	public DataSourceConfigurationException(String msg){
		super(msg);
	}
	public DataSourceConfigurationException(String msg,Throwable t){
		super(msg,t);
	}
	public DataSourceConfigurationException(Throwable t){
		super(t);
	}
}
