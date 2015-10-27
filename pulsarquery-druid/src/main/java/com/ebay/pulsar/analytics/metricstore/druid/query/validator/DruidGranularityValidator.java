/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.validator;

import org.joda.time.DateTime;

import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.InvalidGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.PeriodGranularity;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.ebay.pulsar.analytics.query.validator.QueryValidator;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidGranularityValidator implements QueryValidator<GranularityAndTimeRange> {
	private static long ONEDAY_MILLIS = 86400000;

	@Override
	public void validate(GranularityAndTimeRange req) {
		BaseGranularity granularity = req.getGranularity();
		if (granularity instanceof InvalidGranularity) {
			throw new InvalidQueryParameterException(
					ExceptionErrorCode.INVALID_GRANULARITY.getErrorMessage() + ((InvalidGranularity)granularity).getKey());
		}
		else if(granularity instanceof PeriodGranularity){
			DateRange intervals = req.getIntervals();
			if ("PT1M".equals(((PeriodGranularity)granularity).getPeriod())) {
				DateTime start = intervals.getStart();
				DateTime end = intervals.getEnd();
				long intervalTime = end.getMillis() - start.getMillis();
				if (intervalTime > ONEDAY_MILLIS) {
					throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_GRANULARITY_INTERVAL.getErrorMessage());
				}
			}
		}
	}
}
