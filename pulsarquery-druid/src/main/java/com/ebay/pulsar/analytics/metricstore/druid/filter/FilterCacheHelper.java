/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.filter;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 
 * @author rtao
 *
 */
public class FilterCacheHelper {
	static final byte NOOP_CACHE_ID = -0x4;
	static final byte SELECTOR_CACHE_ID = 0x0;
	static final byte AND_CACHE_ID = 0x1;
	static final byte OR_CACHE_ID = 0x2;
	static final byte NOT_CACHE_ID = 0x3;
	static final byte EXTRACTION_CACHE_ID = 0x4;
	static final byte REGEX_CACHE_ID = 0x5;
	static final byte SEARCH_QUERY_TYPE_ID = 0x6;
	static final byte JAVASCRIPT_CACHE_ID = 0x7;
	static final byte SPATIAL_CACHE_ID = 0x8;

	static byte[] computeCacheKey(byte cacheIdKey, List<BaseFilter> filters) {
		if (filters.size() == 1) {
			return filters.get(0).cacheKey();
		}

		byte[][] cacheKeys = new byte[filters.size()][];
		int totalSize = 0;
		int index = 0;
		for (BaseFilter field : filters) {
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
