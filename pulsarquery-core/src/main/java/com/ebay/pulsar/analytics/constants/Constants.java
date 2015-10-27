/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.constants;


/**
 * 
 * @author rtao
 *
 */
public class Constants {
	public enum RequestNameSpace {
		core,
		realtime,
		sql,
		today,
		yesterday;
	}
	
	public enum Granularity {
		all,
		second,
		minute,
		five_minute,
		fifteen_minute,
		thirty_minute,
		hour,
		day,
		week,
		month
	}
	
	public enum AggregateFunction {
		count,
		countall,	// This is for count(*)
		sum,
		min,
		max;
	}
}
