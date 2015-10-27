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
public class AlphaNumericMetric extends BaseMetric {
	private String previousStop;

	private static final byte CACHE_TYPE_ID = 0x5;

	public AlphaNumericMetric(String previousStop) {
		super(MetricType.alphaNumeric);
		this.previousStop = previousStop;
	}

	public String getPreviousStop() {
		return previousStop;
	}

	@Override
	public byte[] cacheKey() {
		final byte[] cacheKey = previousStop.getBytes();
		return ByteBuffer.allocate(1 + cacheKey.length).put(CACHE_TYPE_ID).put(cacheKey).array();
	}
}
