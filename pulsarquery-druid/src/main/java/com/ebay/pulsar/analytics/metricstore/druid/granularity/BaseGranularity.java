/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.granularity;

/**
 * 
 * @author mingmwang
 *
 */
public abstract class BaseGranularity {
	public static final BaseGranularity ALL = new SimpleGranularity("all");
	public abstract byte[] cacheKey();
}
