/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.aggregator;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.AggregatorType;

/**
 * 
 * @author rtao
 *
 */
public abstract class BaseAggregator {
	private AggregatorType type;
	private String name;
	
	public BaseAggregator(AggregatorType type, String name) {
		this.type = type;
		this.name = name;
	}

	public AggregatorType getType() {
		return type;
	}

	public String getName() {
		return name;
	}
	
	public abstract byte[] cacheKey();

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		BaseAggregator other = (BaseAggregator) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (type != other.type)
			return false;
		return true;
	}
	
	
}
