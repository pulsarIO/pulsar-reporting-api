/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.granularity;

import java.nio.ByteBuffer;

import org.joda.time.DateTime;

import com.google.common.primitives.Longs;

/**
 * 
 * @author rtao
 *
 */
public class DurationGranularity extends BaseGranularity {
	private String duration;
	private String origin = "1970-01-01T00:00:00Z";

	public DurationGranularity(String duration) {
		this.duration = duration;
	}

	public DurationGranularity(String duration, String origin) {
		this.duration = duration;
		this.origin = origin;
	}

	public String getDuration() {
		return duration;
	}

	public String getOrigin() {
		return origin;
	}
	
	public String getType() {
		return "duration";
	}

	@Override
	public byte[] cacheKey() {
		return ByteBuffer.allocate(2 * Longs.BYTES).putLong(Long.valueOf(duration)).putLong(new DateTime(origin).getMillis()).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((duration == null) ? 0 : duration.hashCode());
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
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
		DurationGranularity other = (DurationGranularity) obj;
		if (duration == null) {
			if (other.duration != null)
				return false;
		} else if (!duration.equals(other.duration))
			return false;
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
			return false;
		return true;
	}
	
	
}