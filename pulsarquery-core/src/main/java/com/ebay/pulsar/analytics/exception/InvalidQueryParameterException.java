/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.exception;

public class InvalidQueryParameterException extends RuntimeException{
	private static final long serialVersionUID = -6207679970021851625L;
	
	public InvalidQueryParameterException(String msg){
		super(msg);
	}
	public InvalidQueryParameterException(String msg,Throwable t){
		super(msg,t);
	}
	public InvalidQueryParameterException(Throwable t){
		super(t);
	}

}
