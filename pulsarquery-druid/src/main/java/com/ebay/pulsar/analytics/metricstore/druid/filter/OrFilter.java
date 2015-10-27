/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.filter;

import java.util.List;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.FilterType;

/**
 * 
 * @author rtao
 *
 */
public class OrFilter extends BaseFilter {
	private List<BaseFilter> fields;

	public OrFilter(List<BaseFilter> fields) {
		super(FilterType.or);
		this.fields = fields;
	}

	public List<BaseFilter> getFields() {
		return fields;
	}

	@Override
	public byte[] cacheKey() {
		return FilterCacheHelper.computeCacheKey(FilterCacheHelper.OR_CACHE_ID, fields);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		OrFilter other = (OrFilter) obj;
		if (fields == null) {
			if (other.fields != null)
				return false;
		} else if (!fields.equals(other.fields))
			return false;
		return true;
	}
	
	
}
