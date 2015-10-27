/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.having;

import java.util.List;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.HavingType;

/**
 * 
 * @author rtao
 *
 */
public class AndHaving extends BaseHaving {
	private List<BaseHaving> havingSpecs;

	public AndHaving(List<BaseHaving> havingSpecs) {
		super(HavingType.and);
		this.havingSpecs = havingSpecs;
	}

	public List<BaseHaving> getHavingSpecs() {
		return havingSpecs;
	}

	@Override
	public byte[] cacheKey() {
		return HavingCacheHelper.computeCacheKey(HavingCacheHelper.AND_CACHE_ID, havingSpecs);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((havingSpecs == null) ? 0 : havingSpecs.hashCode());
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
		AndHaving other = (AndHaving) obj;
		if (havingSpecs == null) {
			if (other.havingSpecs != null)
				return false;
		} else if (!havingSpecs.equals(other.havingSpecs))
			return false;
		return true;
	}
	
	
}
