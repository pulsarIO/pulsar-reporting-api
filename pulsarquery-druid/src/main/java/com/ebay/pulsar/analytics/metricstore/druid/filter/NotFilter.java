/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.filter;

import java.nio.ByteBuffer;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.FilterType;

/**
 * 
 * @author rtao
 *
 */
public class NotFilter extends BaseFilter {
	private BaseFilter field;

	public NotFilter(BaseFilter field) {
		super(FilterType.not);
		this.field = field;
	}

	public BaseFilter getField() {
		return field;
	}

	@Override
	public byte[] cacheKey() {
		byte[] subKey = field.cacheKey();
		return ByteBuffer.allocate(1 + subKey.length).put(FilterCacheHelper.NOT_CACHE_ID).put(subKey).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((field == null) ? 0 : field.hashCode());
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
		NotFilter other = (NotFilter) obj;
		if (field == null) {
			if (other.field != null)
				return false;
		} else if (!field.equals(other.field))
			return false;
		return true;
	}
	
	
}
