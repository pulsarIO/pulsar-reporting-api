/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.postaggregator;

import java.nio.ByteBuffer;
import java.util.List;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.PostAggregatorType;
import com.google.common.collect.Lists;

/**
 * 
 * @author rtao
 *
 */
public class ArithmeticPostAggregator extends BasePostAggregator {
	private String fn;
	private List<BasePostAggregator> fields;

	public ArithmeticPostAggregator(String name, String fn, List<BasePostAggregator> fields) {
		super(PostAggregatorType.arithmetic, name);
		this.fn = fn;
		this.fields = fields;
	}
	
	public ArithmeticPostAggregator(String name, String fn, BasePostAggregator... fields) {
		super(PostAggregatorType.arithmetic, name);
		this.fn = fn;
		this.fields = Lists.newArrayList(fields);
	}

	public String getFn() {
		return fn;
	}
	
	public List<BasePostAggregator> getFields() {
		return fields;
	}
	
	@Override
	public byte[] cacheKey() {
		byte[] fnBytes = fn.getBytes();
		byte[] fieldsBytes = PostAggregatorCacheHelper.computeCacheKey(fields);
		return ByteBuffer.allocate(1 + fnBytes.length + fieldsBytes.length).put(PostAggregatorCacheHelper.ARITHMETIC_CACHE_ID).put(fnBytes).put(fieldsBytes).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		result = prime * result + ((fn == null) ? 0 : fn.hashCode());
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
		ArithmeticPostAggregator other = (ArithmeticPostAggregator) obj;
		if (fields == null) {
			if (other.fields != null)
				return false;
		} else if (!fields.equals(other.fields))
			return false;
		if (fn == null) {
			if (other.fn != null)
				return false;
		} else if (!fn.equals(other.fn))
			return false;
		return true;
	}
	
	
}
