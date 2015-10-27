/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.request;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;


/**
 * 
 * @author rtao
 *
 */
public class CoreRequest extends BaseRequest {
	private static final Logger logger = LoggerFactory.getLogger(CoreRequest.class);
	
	private String startTime;
	private String endTime;
	
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	
	@Override
	public DateRange getQueryDateRange() {
		DateTime start = null;
		DateTime end = null;
		try {
			start = PulsarDateTimeFormatter.INPUTTIME_FORMATTER.parseDateTime(getStartTime());
			end = PulsarDateTimeFormatter.INPUTTIME_FORMATTER.parseDateTime(getEndTime());
		} catch (Exception e) {
			logger.warn ("GetQueryDateRange Error:"+ e.getMessage());
		}
		
		if (start == null || (end == null)) {
			throw new InvalidQueryParameterException(
					ExceptionErrorCode.INVALID_QUERYTIME.getErrorMessage() + getStartTime() + "/" + getEndTime());
		}

		if (start.compareTo(end) >= 0) {
			throw new InvalidQueryParameterException(
					ExceptionErrorCode.INVALID_QUERYTIME.getErrorMessage() + getStartTime() + "/" + getEndTime());
		}

		DateTime now = new DateTime(PulsarDateTimeFormatter.MST_TIMEZONE);
		if (start.compareTo(now) >= 0) {
			throw new InvalidQueryParameterException(
					ExceptionErrorCode.INVALID_QUERYTIME.getErrorMessage() + getStartTime() + "/" + getEndTime());
		}

		// If end time is later than current time, set it to current time.
		// Otherwise a partial result will be cached, and later queries will return the same partial result
		if(end.compareTo(now) > 0) {
			end = now;
		}
		
		DateRange dateRange = new DateRange(start, end);
		return dateRange;
	}
}
