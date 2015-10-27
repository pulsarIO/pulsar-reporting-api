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
public class TraceAbleResponse extends QueryResponse {
	private TraceQuery query;
	private long requestProcessTime;

	public long getRequestProcessTime() {
		return requestProcessTime;
	}

	public void setRequestProcessTime(long requestProcessTime) {
		this.requestProcessTime = requestProcessTime;
	}

	public TraceQuery getQuery() {
		return query;
	}

	public void setQuery(TraceQuery query) {
		this.query = query;
	}
}
