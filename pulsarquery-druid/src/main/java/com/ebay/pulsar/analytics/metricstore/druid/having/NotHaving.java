/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.having;

import java.nio.ByteBuffer;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.HavingType;

/**
 * 
 * @author rtao
 *
 */
public class NotHaving extends BaseHaving {
	private BaseHaving havingSpec;

	public NotHaving(BaseHaving havingSpec) {
		super(HavingType.not);
		this.havingSpec = havingSpec;
	}

	public BaseHaving getHavingSpec() {
		return havingSpec;
	}

	@Override
	public byte[] cacheKey() {
		byte[] subKey = havingSpec.cacheKey();
		return ByteBuffer.allocate(1 + subKey.length).put(HavingCacheHelper.NOT_CACHE_ID).put(subKey).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((havingSpec == null) ? 0 : havingSpec.hashCode());
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
		NotHaving other = (NotHaving) obj;
		if (havingSpec == null) {
			if (other.havingSpec != null)
				return false;
		} else if (!havingSpec.equals(other.havingSpec))
			return false;
		return true;
	}
	
	
}
