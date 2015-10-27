/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.limitspec;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.primitives.Ints;

/**
 * 
 * @author rtao
 *
 */
public class DefaultLimitSpec {
	private String type;
	private int limit;
	private List<OrderByColumnSpec> columns;

	private static final byte CACHE_KEY = 0x1;

	public DefaultLimitSpec(int limit, List<OrderByColumnSpec> columns) {
		this.type = "default";
		this.limit = limit;
		this.columns = columns;
	}

	public String getType() {
		return type;
	}

	public int getLimit() {
		return limit;
	}

	public List<OrderByColumnSpec> getColumns() {
		return columns;
	}

	public byte[] cacheKey() {
		final byte[][] columnBytes = new byte[columns.size()][];
		int columnsBytesSize = 0;
		int index = 0;
		for (OrderByColumnSpec column : columns) {
			columnBytes[index] = column.cacheKey();
			columnsBytesSize += columnBytes[index].length;
			++index;
		}

		ByteBuffer buffer = ByteBuffer.allocate(1 + columnsBytesSize + 4).put(CACHE_KEY);
		for (byte[] columnByte : columnBytes) {
			buffer.put(columnByte);
		}
		buffer.put(Ints.toByteArray(limit));
		return buffer.array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columns == null) ? 0 : columns.hashCode());
		result = prime * result + limit;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DefaultLimitSpec other = (DefaultLimitSpec) obj;
		if (columns == null) {
			if (other.columns != null)
				return false;
		} else if (!columns.equals(other.columns))
			return false;
		if (limit != other.limit)
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}
	
	
}
