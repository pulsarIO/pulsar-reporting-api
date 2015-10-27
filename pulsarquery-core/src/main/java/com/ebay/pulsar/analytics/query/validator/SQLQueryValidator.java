/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.validator;

import org.joda.time.DateTime;

import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.query.request.BaseSQLRequest;
import com.ebay.pulsar.analytics.query.request.PulsarDateTimeFormatter;
import com.ebay.pulsar.analytics.query.request.SQLRequest;
import com.google.common.base.Strings;

/**
 * Validate the sql query date time parameters.
 * 
 * @author mingmwang
 *
 */
public class SQLQueryValidator implements QueryValidator<BaseSQLRequest> {
	
	@Override
	public void validate(BaseSQLRequest req) {
		if (Strings.isNullOrEmpty(req.getSql())) {
			throw new InvalidQueryParameterException(ExceptionErrorCode.MISSING_SQL.getErrorMessage());
		}
		
		SQLRequest sqlReq = (SQLRequest) req;

		DateTime start = null;
		DateTime end = null;

		String intervalStr = sqlReq.getIntervals();
		String customTime = sqlReq.getCustomTime();
		String startTime = sqlReq.getStartTime();
		String endTime = sqlReq.getEndTime();

		if (customTime != null) {
			if (startTime == null && endTime == null && intervalStr == null) {
				if (customTime.equals("today")) {
					throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_CUSTOM_TIME.getErrorMessage()+ customTime);
				} else if (customTime.equals("yesterday")) {
					throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_CUSTOM_TIME.getErrorMessage()+ customTime);
				}
			} else {
				throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_QUERYTIME.getErrorMessage());
			}
		} else {
			if (intervalStr != null) {
				String[] strArr = intervalStr.split("/");
				if (strArr.length != 2) {
					throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_QUERYTIME.getErrorMessage());
				} else {
					startTime = strArr[0];
					endTime = strArr[1];
				}
			}
			try {
				start = PulsarDateTimeFormatter.INPUTTIME_FORMATTER.parseDateTime(startTime);
				end = PulsarDateTimeFormatter.INPUTTIME_FORMATTER.parseDateTime(endTime);
			} catch (Exception e) {
			}

			if(start == null) 
				throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_QUERYTIME.getErrorMessage());
			if(end == null) 
				throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_QUERYTIME.getErrorMessage());

			if (start.compareTo(end) >= 0) {
				throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_QUERYTIME.getErrorMessage() + start + "/" + end);
			}
			DateTime now = new DateTime();
			if (start.compareTo(now) >= 0) {
				throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_QUERYTIME.getErrorMessage() + start);
			}
		}
	}
}
