/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.auth.exceptions;
/**
 *@author qxing
 * 
 **/
public class InvalidSessionException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -759779116336619499L;
	public InvalidSessionException(String msg){
		super(msg);
	}
	public InvalidSessionException(String msg,Throwable t){
		super(msg,t);
	}
	public InvalidSessionException(Throwable t){
		super(t);
	}
}
