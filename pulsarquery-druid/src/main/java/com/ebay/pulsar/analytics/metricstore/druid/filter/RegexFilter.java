/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.filter;

import java.nio.ByteBuffer;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.FilterType;
import com.google.common.base.Charsets;

/**
 * 
 * @author rtao
 *
 */
public class RegexFilter extends BaseFilter {
	private String dimension;
	private String pattern;

	public RegexFilter(String dimension, String pattern) {
		super(FilterType.regex);
		this.dimension = dimension;
		this.pattern = pattern;
	}

	public String getDimension() {
		return dimension;
	}

	public String getPattern() {
		return pattern;
	}

	@Override
	public byte[] cacheKey() {
		final byte[] dimensionBytes = dimension.getBytes(Charsets.UTF_8);
		final byte[] patternBytes = pattern.getBytes(Charsets.UTF_8);
		return ByteBuffer.allocate(1 + dimensionBytes.length + patternBytes.length).put(FilterCacheHelper.REGEX_CACHE_ID).put(dimensionBytes).put(patternBytes).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((dimension == null) ? 0 : dimension.hashCode());
		result = prime * result + ((pattern == null) ? 0 : pattern.hashCode());
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
		RegexFilter other = (RegexFilter) obj;
		if (dimension == null) {
			if (other.dimension != null)
				return false;
		} else if (!dimension.equals(other.dimension))
			return false;
		if (pattern == null) {
			if (other.pattern != null)
				return false;
		} else if (!pattern.equals(other.pattern))
			return false;
		return true;
	}
	
	
}
