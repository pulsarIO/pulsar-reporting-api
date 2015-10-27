/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query;

import java.util.List;

import com.ebay.pulsar.analytics.constants.Constants.RequestNameSpace;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.DruidSpecs;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidQueryParameter {
	private DruidSpecs druidSpecs;
	private RequestNameSpace ns;
	private List<String> dbNameSpaces;

	public DruidQueryParameter(DruidSpecs druidSpecs, RequestNameSpace ns) {
		this.druidSpecs = druidSpecs;
		this.ns = ns;
	}
	
	public DruidQueryParameter(DruidSpecs druidSpecs, RequestNameSpace ns, List<String> dbNameSpaces) {
		this.druidSpecs = druidSpecs;
		this.ns = ns;
		this.dbNameSpaces = dbNameSpaces;
	}
	
	public DruidSpecs getDruidSpecs() {
		return druidSpecs;
	}

	public void setDruidSpecs(DruidSpecs druidSpecs) {
		this.druidSpecs = druidSpecs;
	}

	public RequestNameSpace getNs() {
		return ns;
	}

	public void setNs(RequestNameSpace ns) {
		this.ns = ns;
	}

	public List<String> getDbNameSpaces() {
		return dbNameSpaces;
	}

	public void setDbNameSpaces(List<String> dbNameSpaces) {
		this.dbNameSpaces = dbNameSpaces;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dbNameSpaces == null) ? 0 : dbNameSpaces.hashCode());
		result = prime * result
				+ ((druidSpecs == null) ? 0 : druidSpecs.hashCode());
		result = prime * result + ((ns == null) ? 0 : ns.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DruidQueryParameter other = (DruidQueryParameter) obj;
		if (dbNameSpaces == null) {
			if (other.dbNameSpaces != null)
				return false;
		} else if (!dbNameSpaces.equals(other.dbNameSpaces))
			return false;
		if (druidSpecs == null) {
			if (other.druidSpecs != null)
				return false;
		} else if (!druidSpecs.equals(other.druidSpecs))
			return false;
		if (ns != other.ns)
			return false;
		return true;
	}
}