/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.filter;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.FilterType;

/**
 * 
 * @author rtao
 *
 */
public abstract class BaseFilter {
	private FilterType type;

	public BaseFilter(FilterType type) {
		this.type = type;
	}

	public FilterType getType() {
		return type;
	}

	public abstract byte[] cacheKey();

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		BaseFilter other = (BaseFilter) obj;
		if (type != other.type)
			return false;
		return true;
	}
	
	
}
