/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.exception;


public class SqlTranslationException  extends RuntimeException{
	private static final long serialVersionUID = 4476978734547934067L;
	
	public SqlTranslationException(String msg){
		super(msg);
	}
	public SqlTranslationException(String msg,Throwable t){
		super(msg,t);
	}
	public SqlTranslationException(Throwable t){
		super(t);
	}
}
