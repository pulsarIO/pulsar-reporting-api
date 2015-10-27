/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.exception;


public class DataSourceException extends RuntimeException  {
	private static final long serialVersionUID = 3550626010738416585L;
	
	public DataSourceException(String msg){
		super(msg);
	}
	public DataSourceException(String msg,Throwable t){
		super(msg,t);
	}
	public DataSourceException(Throwable t){
		super(t);
	}

}
