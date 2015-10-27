/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.resources.exception;


public class InvalidAPIParameterException extends RuntimeException{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6486713606693730634L;
	public InvalidAPIParameterException(String msg){
		super(msg);
	}
	public InvalidAPIParameterException(String msg,Throwable t){
		super(msg,t);
	}
	public InvalidAPIParameterException(Throwable t){
		super(t);
	}

}

