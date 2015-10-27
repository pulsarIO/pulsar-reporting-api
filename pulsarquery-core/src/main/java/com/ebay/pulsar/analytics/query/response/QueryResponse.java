/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.response;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author rtao
 *
 */
public class QueryResponse {
	private List<Map<String, Object>> queryResult;

	public List<Map<String, Object>> getQueryResult() {
		return queryResult;
	}
	public void setQueryResult(List<Map<String, Object>> queryResult) {
		this.queryResult = queryResult;
	}

}
