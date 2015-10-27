/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.request;


/**
 * 
 * @author rtao
 *
 */
public class SQLRequest extends BaseSQLRequest {
	private String startTime;
	private String endTime;
	private String customTime;
	private String intervals;

	public String getIntervals() {
		return intervals;
	}
	public void setIntervals(String intervals) {
		this.intervals = intervals;
	}
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
	public String getCustomTime() {
		return customTime;
	}
	public void setCustomTime(String customTime) {
		this.customTime = customTime;
	}
}
