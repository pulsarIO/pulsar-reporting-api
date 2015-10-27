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
public class LexicographicMetric extends BaseMetric {
	private String previousStop;

	private static final byte CACHE_TYPE_ID = 0x1;
	
	public LexicographicMetric(String previousStop) {
		super(MetricType.lexicographic);
		this.previousStop = previousStop;
	}

	public String getPreviousStop() {
		return previousStop;
	}

	@Override
	public byte[] cacheKey() {
		byte[] previousStopBytes = previousStop == null ? new byte[] {} : previousStop.getBytes(Charsets.UTF_8);
		return ByteBuffer.allocate(1 + previousStopBytes.length).put(CACHE_TYPE_ID).put(previousStopBytes).array();
	}
}
