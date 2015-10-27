/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.having;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 
 * @author rtao
 *
 */
public class HavingCacheHelper {
	static final byte NOOP_CACHE_ID = -0x4;
	static final byte EQUAL_TO_CACHE_ID = 0x0;
	static final byte GREATER_THAN_CACHE_ID = 0x1;
	static final byte LESS_THAN_CACHE_ID = 0x2;
	static final byte AND_CACHE_ID = 0x3;
	static final byte OR_CACHE_ID = 0x4;
	static final byte NOT_CACHE_ID = 0x5;

	static byte[] computeCacheKey(byte cacheIdKey, List<BaseHaving> havingSpecs) {
		if (havingSpecs.size() == 1) {
			return havingSpecs.get(0).cacheKey();
		}

		byte[][] cacheKeys = new byte[havingSpecs.size()][];
		int totalSize = 0;
		int index = 0;
		for (BaseHaving field : havingSpecs) {
			cacheKeys[index] = field.cacheKey();
			totalSize += cacheKeys[index].length;
			++index;
		}

		ByteBuffer retVal = ByteBuffer.allocate(1 + totalSize);
		retVal.put(cacheIdKey);
		for (byte[] cacheKey : cacheKeys) {
			retVal.put(cacheKey);
		}
		return retVal.array();
	}
}
