/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.result;

/**
 * Implement this interface to do alias transformation for query result.
 *
 * @author mingmwang
 * 
 */
public interface ResultRevisor {
	public void revise(ResultNode node); 
}
