/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.validator;

import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.query.request.DateRange;

/**
 * 
 * @author mingmwang
 *
 */
public class GranularityAndTimeRange {
	private BaseGranularity granularity;
	private DateRange intervals;
	
	public GranularityAndTimeRange(BaseGranularity granularity, DateRange intervals){
		this.granularity = granularity;
		this.intervals = intervals;
	}
	public BaseGranularity getGranularity() {
		return granularity;
	}
	
	public DateRange getIntervals() {
		return intervals;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((granularity == null) ? 0 : granularity.hashCode());
		result = prime * result
				+ ((intervals == null) ? 0 : intervals.hashCode());
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
		GranularityAndTimeRange other = (GranularityAndTimeRange) obj;
		if (granularity == null) {
			if (other.granularity != null)
				return false;
		} else if (!granularity.equals(other.granularity))
			return false;
		if (intervals == null) {
			if (other.intervals != null)
				return false;
		} else if (!intervals.equals(other.intervals))
			return false;
		return true;
	}
}
