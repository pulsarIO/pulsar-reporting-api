/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.holap.query.model;

import com.ebay.pulsar.analytics.datasource.PulsarRestMetricMeta;
import com.ebay.pulsar.analytics.query.request.DateRange;

/**
 * 
 * @author mingmwang
 *
 */
public class QueryMetricProps {
	private PulsarRestMetricMeta metricMeta;
	private String metric;
	private DateRange intervals;

	public QueryMetricProps (PulsarRestMetricMeta metricMeta, DateRange intervals) {
		this.metricMeta = metricMeta;
		this.metric = metricMeta.getMetricName();
		this.intervals = intervals;
	}

	public PulsarRestMetricMeta getMetricMeta () {
		return this.metricMeta;
	}

	public String getMetric () {
		return this.metric;
	}

	public DateRange getIntevals () {
		return this.intervals;
	}
}
