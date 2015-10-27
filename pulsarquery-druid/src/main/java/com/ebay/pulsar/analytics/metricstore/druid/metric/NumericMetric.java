/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.metric;

import java.nio.ByteBuffer;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.MetricType;
import com.google.common.base.Charsets;

/**
 * 
 * @author rtao
 *
 */
public class NumericMetric extends BaseMetric {
	private String metric;
	
	private static final byte CACHE_TYPE_ID = 0x0;

	public NumericMetric(String metric) {
		super(MetricType.numeric);
		this.metric = metric;
	}

	public String getMetric() {
		return metric;
	}

	@Override
	public byte[] cacheKey() {
		byte[] metricBytes = metric.getBytes(Charsets.UTF_8);
		return ByteBuffer.allocate(1 + metricBytes.length).put(CACHE_TYPE_ID).put(metricBytes).array();
	}
}
