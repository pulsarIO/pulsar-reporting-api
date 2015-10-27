/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;


import java.util.Set;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidRestTableMeta {
	private Set<String> dimensions; 
	private Set<String> metrics;
	
	public Set<String> getDimensions() {
		return dimensions;
	}
	public void setDimensions(Set<String> dimensions) {
		this.dimensions = dimensions;
	}
	public Set<String> getMetrics() {
		return metrics;
	}
	public void setMetrics(Set<String> metrics) {
		this.metrics = metrics;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dimensions == null) ? 0 : dimensions.hashCode());
		result = prime * result + ((metrics == null) ? 0 : metrics.hashCode());
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
		DruidRestTableMeta other = (DruidRestTableMeta) obj;
		if (dimensions == null) {
			if (other.dimensions != null)
				return false;
		} else if (!dimensions.equals(other.dimensions))
			return false;
		if (metrics == null) {
			if (other.metrics != null)
				return false;
		} else if (!metrics.equals(other.metrics))
			return false;
		return true;
	}
}
