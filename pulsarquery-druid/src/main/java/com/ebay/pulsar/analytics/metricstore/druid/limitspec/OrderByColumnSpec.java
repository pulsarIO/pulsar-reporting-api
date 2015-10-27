/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.limitspec;

/**
 * 
 * @author rtao
 *
 */
import java.nio.ByteBuffer;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.SortDirection;

public class OrderByColumnSpec {
	private String dimension;
	private SortDirection direction;
	
	public OrderByColumnSpec(String dimension, SortDirection direction) {
		this.dimension = dimension;
		this.direction = direction;
	}
	
	public String getDimension() {
		return dimension;
	}
	
	public SortDirection getDirection() {
		return direction;
	}
	
	public byte[] cacheKey() {
		final byte[] dimensionBytes = dimension.getBytes();
		final byte[] directionBytes = direction.name().getBytes();

		return ByteBuffer.allocate(dimensionBytes.length + directionBytes.length).put(dimensionBytes).put(directionBytes).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dimension == null) ? 0 : dimension.hashCode());
		result = prime * result
				+ ((direction == null) ? 0 : direction.hashCode());
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
		OrderByColumnSpec other = (OrderByColumnSpec) obj;
		if (dimension == null) {
			if (other.dimension != null)
				return false;
		} else if (!dimension.equals(other.dimension))
			return false;
		if (direction != other.direction)
			return false;
		return true;
	}
	
}
