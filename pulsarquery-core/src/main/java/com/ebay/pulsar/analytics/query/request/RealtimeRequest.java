/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.request;

import org.joda.time.DateTime;


/**
 * 
 * @author rtao
 *
 */
public class RealtimeRequest extends BaseRequest {
	private Integer duration;

	public Integer getDuration() {
		return duration;
	}

	public void setDuration(Integer duration) {
		this.duration = duration;
	}

	@Override
	public DateRange getQueryDateRange() {
		DateTime start = null;
		DateTime end = null;
		DateTime now = new DateTime(PulsarDateTimeFormatter.MST_TIMEZONE);
		
		// normalize to 0, 10, 20, 30, 40, 50 of each minute
		end = new DateTime((now.getMillis() /10000)*10000);
		Integer duration = getDuration();
		if (duration == null) {
			duration = 5 * 60; // default to last 5 minutes
		}
		start = new DateTime(end.getMillis() - duration * 1000);
		
		DateRange dateRange = new DateRange(start, end);
		return dateRange;
	}
}
