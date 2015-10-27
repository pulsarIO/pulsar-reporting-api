/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.request;

import com.ebay.pulsar.analytics.constants.Constants.RequestNameSpace;

/**
 * 
 * @author rtao
 *
 */
public abstract class BaseSQLRequest {
	private String sql;
	private String granularity;
	private RequestNameSpace namespace;

	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getGranularity() {
		return granularity;
	}
	public void setGranularity(String granularity) {
		this.granularity = granularity;
	}

	public RequestNameSpace getNamespace() {
		return namespace;
	}
	public void setNamespace(RequestNameSpace namespace) {
		this.namespace = namespace;
	}
}
