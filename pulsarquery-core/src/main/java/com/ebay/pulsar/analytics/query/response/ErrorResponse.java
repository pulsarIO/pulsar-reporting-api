/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.response;

/**
 * 
 * @author rtao
 * 
 */
public class ErrorResponse {

	public String url;
	public String exception;

	/**
	 * @param exception
	 */
	public ErrorResponse(String url, Exception exception) {
		this.url = url;
		this.exception = exception.getLocalizedMessage();
	}

}
