/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query;

import java.util.List;

import com.ebay.pulsar.analytics.constants.Constants.RequestNameSpace;
import com.ebay.pulsar.analytics.query.request.DateRange;

/**
 * 
 * @author mingmwang
 *
 */
public class SQLQueryContext {
	private DateRange intervals;
	private String sqlQuery;
	private String granularity;
	private RequestNameSpace ns;

	private List<String> tableNames;
	private List<String> dbNameSpaces;

	public RequestNameSpace getNs() {
		return ns;
	}

	public void setNs(RequestNameSpace ns) {
		this.ns = ns;
	}

	public DateRange getIntervals() {
		return intervals;
	}

	public void setIntervals(DateRange intervals) {
		this.intervals = intervals;
	}

	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public String getGranularity() {
		return granularity;
	}

	public void setGranularity(String granularity) {
		this.granularity = granularity;
	}

	public List<String> getTableNames() {
		return tableNames;
	}

	public void setTableNames(List<String> tableNames) {
		this.tableNames = tableNames;
	}

	public List<String> getDbNameSpaces() {
		return dbNameSpaces;
	}

	public void setDbNameSpaces(List<String> dbNameSpaces) {
		this.dbNameSpaces = dbNameSpaces;
	}

}
