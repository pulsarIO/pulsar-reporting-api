/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.model;

import java.nio.ByteBuffer;
import java.util.List;

import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.google.common.collect.Lists;

/**
 * 
 * @author rtao
 *
 */
public class QueryCacheHelper {
	private static final byte CACHE_TYPE_ID = 0x1; // used for String

	public static byte[] computeAggregatorBytes(List<BaseAggregator> aggregators) {
		List<byte[]> cacheKeySet = Lists.newArrayListWithCapacity(aggregators.size());

		int totalSize = 0;
		for (BaseAggregator agg : aggregators) {
			final byte[] cacheKey = agg.cacheKey();
			cacheKeySet.add(cacheKey);
			totalSize += cacheKey.length;
		}

		ByteBuffer retVal = ByteBuffer.allocate(totalSize);
		for (byte[] bytes : cacheKeySet) {
			retVal.put(bytes);
		}
		return retVal.array();
	}
	
	public static byte[] computePostAggregatorBytes(List<BasePostAggregator> postaggregators) {
		List<byte[]> cacheKeySet = Lists.newArrayListWithCapacity(postaggregators.size());

		int totalSize = 0;
		for (BasePostAggregator agg : postaggregators) {
			final byte[] cacheKey = agg.cacheKey();
			cacheKeySet.add(cacheKey);
			totalSize += cacheKey.length;
		}

		ByteBuffer retVal = ByteBuffer.allocate(totalSize);
		for (byte[] bytes : cacheKeySet) {
			retVal.put(bytes);
		}
		return retVal.array();
	}

	public static byte[] computeIntervalsBytes(List<String> intervals) {
		List<byte[]> cacheKeySet = Lists.newArrayListWithCapacity(intervals.size());

		int totalSize = 0;
		for (String interval : intervals) {
			final byte[] cacheKey = computeStringBytes(interval);
			cacheKeySet.add(cacheKey);
			totalSize += cacheKey.length;
		}

		ByteBuffer retVal = ByteBuffer.allocate(totalSize);
		for (byte[] bytes : cacheKeySet) {
			retVal.put(bytes);
		}
		return retVal.array();
	}

	private static byte[] computeStringBytes(String str) {
		byte[] stringBytes = str.getBytes();
		return ByteBuffer.allocate(1 + stringBytes.length).put(CACHE_TYPE_ID).put(stringBytes).array();
	}
}
