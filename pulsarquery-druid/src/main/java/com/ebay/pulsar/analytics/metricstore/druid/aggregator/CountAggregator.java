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
public class CountAggregator extends BaseAggregator {
	private static final byte CACHE_TYPE_ID = 0x0;

	public CountAggregator(String name) {
		super(AggregatorType.count, name);
	}

	@Override
	public byte[] cacheKey() {
		byte[] nameBytes = super.getName().getBytes();
		return ByteBuffer.allocate(1 + nameBytes.length).put(CACHE_TYPE_ID).put(nameBytes).array();
	}
}
