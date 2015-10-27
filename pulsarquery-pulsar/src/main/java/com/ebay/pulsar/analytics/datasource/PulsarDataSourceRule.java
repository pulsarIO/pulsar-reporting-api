/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarDataSourceRule {
	private String datasourceType;
	private String period;

	public PulsarDataSourceRule(String datasourceType, String period) {
		this.datasourceType = datasourceType;
		this.period = period;
	}

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}
	
	public String getDatasourceType() {
		return datasourceType;
	}

	public void setDatasourceType(String datasourceType) {
		this.datasourceType = datasourceType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((period == null) ? 0 : period.hashCode());
		result = prime * result
				+ ((datasourceType == null) ? 0 : datasourceType.hashCode());
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
		PulsarDataSourceRule other = (PulsarDataSourceRule) obj;
		if (period == null) {
			if (other.period != null)
				return false;
		} else if (!period.equals(other.period))
			return false;
		if (datasourceType == null) {
			if (other.datasourceType != null)
				return false;
		} else if (!datasourceType.equals(other.datasourceType))
			return false;
		return true;
	}
}
