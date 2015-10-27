/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.granularity;

import java.nio.ByteBuffer;

/**
 * 
 * @author rtao
 *
 */
public class PeriodGranularity extends BaseGranularity {
	private String period;
	private String timeZone;
	private String origin;

	public PeriodGranularity(String period) {
		this(period, "UTC");
	}

	public PeriodGranularity(String period, String timeZone) {
		this(period, timeZone, "1970-01-01T00:00:00");
	}

	public PeriodGranularity(String period, String timeZone, String origin) {
		this.period = period;
		this.timeZone = timeZone;
		this.origin = origin;
	}

	public String getPeriod() {
		return period;
	}

	public String getTimeZone() {
		return timeZone;
	}

	public String getOrigin() {
		return origin;
	}
	
	public String getType() {
		return "period";
	}

	@Override
	public byte[] cacheKey() {
		byte[] periodBytes = period.getBytes();
		byte[] timeZoneBytes = timeZone.getBytes();
		byte[] originBytes = origin.getBytes();
		return ByteBuffer.allocate(periodBytes.length + timeZoneBytes.length + originBytes.length).put(periodBytes).put(timeZoneBytes).put(originBytes).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
		result = prime * result + ((period == null) ? 0 : period.hashCode());
		result = prime * result
				+ ((timeZone == null) ? 0 : timeZone.hashCode());
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
		PeriodGranularity other = (PeriodGranularity) obj;
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
			return false;
		if (period == null) {
			if (other.period != null)
				return false;
		} else if (!period.equals(other.period))
			return false;
		if (timeZone == null) {
			if (other.timeZone != null)
				return false;
		} else if (!timeZone.equals(other.timeZone))
			return false;
		return true;
	}
	
	
}
