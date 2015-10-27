/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.aggregator;

import java.nio.ByteBuffer;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.AggregatorType;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;

/**
 * 
 * @author rtao
 *
 */
public class FilteredAggregator extends BaseAggregator {
	private BaseFilter filter;
	private BaseAggregator aggregator;
	
	private static final byte CACHE_TYPE_ID = 0x7;

	public FilteredAggregator(String name, BaseFilter filter, BaseAggregator aggregator) {
		super(AggregatorType.filtered, name);
		this.filter = filter;
		this.aggregator = aggregator;
	}

	public BaseFilter getFilter() {
		return filter;
	}

	public BaseAggregator getAggregator() {
		return aggregator;
	}

	@Override
	public byte[] cacheKey() {
		byte[] nameBytes = super.getName().getBytes();
		byte[] filterBytes = filter.cacheKey();
		byte[] aggregatorBytes = aggregator.cacheKey();

		return ByteBuffer.allocate(1 + nameBytes.length + filterBytes.length + aggregatorBytes.length)
				.put(CACHE_TYPE_ID).put(nameBytes).put(filterBytes).put(aggregatorBytes).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((aggregator == null) ? 0 : aggregator.hashCode());
		result = prime * result + ((filter == null) ? 0 : filter.hashCode());
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
		FilteredAggregator other = (FilteredAggregator) obj;
		if (aggregator == null) {
			if (other.aggregator != null)
				return false;
		} else if (!aggregator.equals(other.aggregator))
			return false;
		if (filter == null) {
			if (other.filter != null)
				return false;
		} else if (!filter.equals(other.filter))
			return false;
		return true;
	}
	
	
}
