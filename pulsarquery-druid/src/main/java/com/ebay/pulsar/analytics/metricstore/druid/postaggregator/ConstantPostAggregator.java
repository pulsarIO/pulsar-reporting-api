/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.postaggregator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.PostAggregatorType;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

/**
 * 
 * @author rtao
 *
 */
public class ConstantPostAggregator extends BasePostAggregator {
	private Number value;

	public ConstantPostAggregator(String name, Number value) {
		super(PostAggregatorType.constant, name);
		this.value = value;
	}

	public Number getValue() {
		return value;
	}
	
	@Override
	public byte[] cacheKey() {
		// Depending on the Number subclass type
		if (this.value instanceof Long) {
			ByteBuffer buffer = ByteBuffer.allocate(1 + 8).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).put(Longs.toByteArray((Long)value));
			return buffer.array();
		} else if (this.value instanceof Integer) {
			ByteBuffer buffer = ByteBuffer.allocate(1 + 4).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).put(Ints.toByteArray((Integer)value));
			return buffer.array();
		} else if (this.value instanceof Short) {
			ByteBuffer buffer = ByteBuffer.allocate(1 + 2).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).put(Shorts.toByteArray((Short)value));
			return buffer.array();
		} else if (this.value instanceof Byte) {
			ByteBuffer buffer = ByteBuffer.allocate(1 + 1).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).put((Byte)value);
			return buffer.array();
		} else if (this.value instanceof Double) {
			ByteBuffer buffer = ByteBuffer.allocate(1 + 8).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).putDouble((Double)value);
			return buffer.array();
		} else if (this.value instanceof Float) {
			ByteBuffer buffer = ByteBuffer.allocate(1 + 4).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).putFloat((Float)value);
			return buffer.array();
		} else if (this.value instanceof BigDecimal) {
			Double bigDouble = this.value.doubleValue();
			ByteBuffer buffer = ByteBuffer.allocate(1 + 8).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).putDouble((Double)bigDouble);
			return buffer.array();
		} else if (this.value instanceof BigInteger) {
			byte[] bigIntBytes = ((BigInteger) this.value).toByteArray();
			int len = bigIntBytes.length;
			ByteBuffer buffer = ByteBuffer.allocate(1 + len).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).put(bigIntBytes);
			return buffer.array();
		} else if (this.value instanceof AtomicInteger) {
			int intVal = this.value.intValue();
			ByteBuffer buffer = ByteBuffer.allocate(1 + 4).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).put(Ints.toByteArray(intVal));
			return buffer.array();
		} else {
			long longVal = this.value.longValue();
			ByteBuffer buffer = ByteBuffer.allocate(1 + 8).put(PostAggregatorCacheHelper.CONSTANT_CACHE_ID).put(Longs.toByteArray(longVal));
			return buffer.array();
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
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
		ConstantPostAggregator other = (ConstantPostAggregator) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
}
