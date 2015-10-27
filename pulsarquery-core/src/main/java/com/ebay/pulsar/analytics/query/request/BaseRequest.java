/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.request;

import java.util.List;

import com.ebay.pulsar.analytics.constants.Constants.RequestNameSpace;

/**
 * 
 * @author rtao
 *
 */
public abstract class BaseRequest {
	private List<String> metrics;
	private List<String> dimensions;
	private String filter;
	private String granularity;
	private Integer maxResults;
	private String sort;
	private String having;
	private RequestNameSpace namespace;
	
	public List<String> getMetrics() {
		return metrics;
	}
	public void setMetrics(List<String> metrics) {
		this.metrics = metrics;
	}
	public List<String> getDimensions() {
		return dimensions;
	}
	public void setDimensions(List<String> dimensions) {
		this.dimensions = dimensions;
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}
	public String getHaving() {
		return having;
	}
	public void setHaving(String having) {
		this.having = having;
	}
	public String getGranularity() {
		return granularity;
	}
	public void setGranularity(String granularity) {
		this.granularity = granularity;
	}
	public Integer getMaxResults() {
		return maxResults;
	}
	public void setMaxResults(Integer maxResults) {
		this.maxResults = maxResults;
	}
	public String getSort() {
		return sort;
	}
	public void setSort(String sort) {
		this.sort = sort;
	}
	public RequestNameSpace getNamespace() {
		return namespace;
	}
	public void setNamespace(RequestNameSpace namespace) {
		this.namespace = namespace;
	}
	
	public abstract DateRange getQueryDateRange();
}
