/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.postaggregator;

import java.nio.ByteBuffer;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.PostAggregatorType;

/**
 * 
 * @author rtao
 *
 */
public class FieldAccessorPostAggregator extends BasePostAggregator {
	private String fieldName;

	public FieldAccessorPostAggregator(String name, String fieldName) {
		super(PostAggregatorType.fieldAccess, name);
		this.fieldName = fieldName;
	}

	public String getFieldName() {
		return fieldName;
	}
	
	@Override
	public byte[] cacheKey() {
		byte[] fieldNameBytes = fieldName.getBytes();
		return ByteBuffer.allocate(1 + fieldNameBytes.length).put(PostAggregatorCacheHelper.FIELD_ACCESSOR_CACHE_ID).put(fieldNameBytes).array();
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
		FieldAccessorPostAggregator other = (FieldAccessorPostAggregator) obj;
		if (fieldName == null) {
			if (other.fieldName != null)
				return false;
		} else if (!fieldName.equals(other.fieldName))
			return false;
		return true;
	}
	
	
}
