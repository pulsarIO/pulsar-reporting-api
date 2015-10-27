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
public class EqualToHaving extends BaseHaving {
	private String aggregation;
	private String value;

	public EqualToHaving(String aggregation, String value) {
		super(HavingType.equalTo);
		this.aggregation = aggregation;
		this.value = value;
	}
	
	public String getAggregation() {
		return aggregation;
	}

	public String getValue() {
		return value;
	}

	@Override
	public byte[] cacheKey() {
		byte[] aggregationBytes = aggregation.getBytes();
		byte[] valueBytes = value == null ? new byte[] {} : value.getBytes();
		return ByteBuffer.allocate(1 + aggregationBytes.length + valueBytes.length).put(HavingCacheHelper.EQUAL_TO_CACHE_ID).put(aggregationBytes).put(valueBytes).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((aggregation == null) ? 0 : aggregation.hashCode());
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
		EqualToHaving other = (EqualToHaving) obj;
		if (aggregation == null) {
			if (other.aggregation != null)
				return false;
		} else if (!aggregation.equals(other.aggregation))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	
	
}
