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
public class SelectorFilter extends BaseFilter {
	private String dimension;
	private String value;

	public SelectorFilter(String dimension, String value) {
		super(FilterType.selector);
		this.dimension = dimension;
		this.value = value;
	}

	public String getDimension() {
		return dimension;
	}

	public String getValue() {
		return value;
	}

	@Override
	public byte[] cacheKey() {
		byte[] dimensionBytes = dimension.getBytes();
		byte[] valueBytes = value == null ? new byte[] {} : value.getBytes();
		return ByteBuffer.allocate(1 + dimensionBytes.length + valueBytes.length).put(FilterCacheHelper.SELECTOR_CACHE_ID).put(dimensionBytes).put(valueBytes).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((dimension == null) ? 0 : dimension.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		SelectorFilter other = (SelectorFilter) obj;
		if (dimension == null) {
			if (other.dimension != null)
				return false;
		} else if (!dimension.equals(other.dimension))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	
	
}
