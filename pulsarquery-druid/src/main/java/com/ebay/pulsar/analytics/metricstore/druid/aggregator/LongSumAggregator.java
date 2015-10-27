/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.aggregator;

import java.nio.ByteBuffer;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.AggregatorType;

/**
 * 
 * @author rtao
 *
 */
public class LongSumAggregator extends BaseAggregator {
	private String fieldName;

	private static final byte CACHE_TYPE_ID = 0x1;

	public LongSumAggregator(String name, String fieldName) {
		super(AggregatorType.longSum, name);
		this.fieldName = fieldName;
	}

	public String getFieldName() {
		return fieldName;
	}

	@Override
	public byte[] cacheKey() {
		byte[] nameBytes = super.getName().getBytes();
		byte[] fieldNameBytes = fieldName.getBytes();

		return ByteBuffer.allocate(1 + nameBytes.length + fieldNameBytes.length).put(CACHE_TYPE_ID)
				.put(nameBytes).put(fieldNameBytes).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((fieldName == null) ? 0 : fieldName.hashCode());
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
		LongSumAggregator other = (LongSumAggregator) obj;
		if (fieldName == null) {
			if (other.fieldName != null)
				return false;
		} else if (!fieldName.equals(other.fieldName))
			return false;
		return true;
	}
	
	
}
