/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.granularity;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 
 * @author mingmwang
 *
 */
public class SimpleGranularity extends BaseGranularity {
	private final String key;
	public SimpleGranularity(String key){
		this.key = key;
	}
	
	@Override
	public byte[] cacheKey() {
		return key.getBytes();
	}

	@JsonValue
	public String getKey() {
		return key;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
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
		SimpleGranularity other = (SimpleGranularity) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}
	
}
