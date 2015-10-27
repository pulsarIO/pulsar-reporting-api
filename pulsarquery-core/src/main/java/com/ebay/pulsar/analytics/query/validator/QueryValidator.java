/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.validator;

/**
 * Validate the query parameters.
 * 
 * @author mingmwang
 *
 */
public interface QueryValidator<T> {
	 void validate(T part);
}
