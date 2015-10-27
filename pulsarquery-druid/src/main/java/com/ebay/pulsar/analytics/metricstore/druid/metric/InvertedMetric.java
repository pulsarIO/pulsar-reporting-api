/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.metric;

import java.nio.ByteBuffer;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.MetricType;

/**
 * 
 * @author rtao
 *
 */
public class InvertedMetric extends BaseMetric {
	private BaseMetric metric;
	
	private static final byte CACHE_TYPE_ID = 0x3;

	public InvertedMetric(BaseMetric metric) {
		super(MetricType.inverted);
		this.metric = metric;
	}

	public BaseMetric getMetric() {
		return metric;
	}

	@Override
	public byte[] cacheKey() {
		final byte[] cacheKey = metric.cacheKey();
		return ByteBuffer.allocate(1 + cacheKey.length).put(CACHE_TYPE_ID).put(cacheKey).array();
	}
}
