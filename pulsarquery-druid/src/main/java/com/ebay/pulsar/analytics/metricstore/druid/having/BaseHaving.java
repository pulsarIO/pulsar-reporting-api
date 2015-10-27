/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.having;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.HavingType;

/**
 * 
 * @author rtao
 *
 */
public abstract class BaseHaving {
	private HavingType type;
	
	public BaseHaving(HavingType type) {
		this.type = type;
	}

	public HavingType getType() {
		return type;
	}

	public abstract byte[] cacheKey();

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		BaseHaving other = (BaseHaving) obj;
		if (type != other.type)
			return false;
		return true;
	}
	
	
}
