/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.exception;


import com.google.common.base.Joiner;

public enum ExceptionErrorCode {
	DATASOURCE_ERROR(11001, "DataSource Error. "),
	INVALID_DATASOURCE(11002, "Invalid query datasource:"),
	INVALID_ENDPOINTS(11003, "Invalid Endpoints in XML config for metric: "),
	
	INVALID_GRANULARITY(12001, "Invalid granularity: "),
	INVALID_GRANULARITY_INTERVAL(12002, "Invalid granularity minute with Intervals > 1 day. "),
	INVALID_QUERYTIME(12003, "Invalid time range (start/end):"),	
	INVALID_CUSTOM_TIME(12004, "Invalid Custom Time: "),

	MISSING_SQL(13001, "Missing sql."),
	SQL_PARSING_ERROR(13002, "SQL parsing error: "),
	INVALID_AGGREGATE(13011, "Invalid aggregate: "),
	INVALID_SORT_PARAM(13021, "Invalid sort parameter (must be metric/aggregate): "),
	INVALID_FILTER(13031, "Invalid filter: "),
	MISSING_METRIC(13041, "Missing metrics. "),
	INVALID_METRIC(13042, "Invalid metrics: "),
	MULTI_METRICS_ERROR(13043, "Multi-metrics not supported. "),
	INVALID_HAVING_CLAUSE(13051, "Invalid having clause: "),
	INVALID_DIMENSION(13061, "Invalid dimension: "),
	INVALID_MAXRESULT(13071, "Invalid maxResults parameter: "),
	INVALID_DURATION(13081, "Invalid duration (must be between 1 to 1800 seconds): "),

	;
	
	private final int code;
	private final String message;
	private final String errorMessage;
	
	private ExceptionErrorCode(int code, String message){
        this.code = code;
        this.message = message;
        StringBuilder sb = new StringBuilder();
        Joiner.on(":").appendTo(sb, code, message);
        errorMessage = sb.toString();
    }
	
	public String getErrorMessage(){
		return errorMessage;
	}
	
	public int getCode(){
		return code;
	}
	
	public String getMessage(){
		return message;
	}
}

