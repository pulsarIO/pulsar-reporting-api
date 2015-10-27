/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.metric;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.MetricType;

/**
 * 
 * @author rtao
 *
 */
public abstract class BaseMetric {
	private MetricType type;

	public BaseMetric(MetricType type) {
		this.type = type;
	}

	public MetricType getType() {
		return type;
	}

	public abstract byte[] cacheKey();
}
