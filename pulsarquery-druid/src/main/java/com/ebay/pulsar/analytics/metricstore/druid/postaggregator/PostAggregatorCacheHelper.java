/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.postaggregator;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 
 * @author rtao
 *
 */
public class PostAggregatorCacheHelper {
	static final byte NOOP_CACHE_ID = -0x4;
	static final byte ARITHMETIC_CACHE_ID = 0x0;
	static final byte FIELD_ACCESSOR_CACHE_ID = 0x1;
	static final byte CONSTANT_CACHE_ID = 0x2;
	static final byte HYPERUNIQUECARDINALITY_CACHE_ID = 0x4;

	static byte[] computeCacheKey(List<BasePostAggregator> fields) {
		if (fields.size() == 1) {
			return fields.get(0).cacheKey();
		}

		byte[][] cacheKeys = new byte[fields.size()][];
		int totalSize = 0;
		int index = 0;
		for (BasePostAggregator field : fields) {
			cacheKeys[index] = field.cacheKey();
			totalSize += cacheKeys[index].length;
			++index;
		}

		ByteBuffer retVal = ByteBuffer.allocate(totalSize);
		for (byte[] cacheKey : cacheKeys) {
			retVal.put(cacheKey);
		}
		return retVal.array();
	}

}
