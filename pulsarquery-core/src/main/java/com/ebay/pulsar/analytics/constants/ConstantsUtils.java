/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.constants;

import java.util.EnumMap;
import java.util.Map;

import org.apache.commons.lang3.EnumUtils;

import com.ebay.pulsar.analytics.constants.Constants.AggregateFunction;
import com.ebay.pulsar.analytics.constants.Constants.Granularity;

/**
 * 
 * @author mingmwang
 *
 */
public class ConstantsUtils {
	private static Map<String, Granularity> mapGranularity = EnumUtils.getEnumMap(Granularity.class);
	private static EnumMap<Granularity, String> granularityDurationMap = new EnumMap<Granularity ,String>(Granularity.class);
	
	static{
		granularityDurationMap.put(Granularity.second, "PT1S");
		granularityDurationMap.put(Granularity.minute, "PT1M");
		granularityDurationMap.put(Granularity.five_minute, "PT5M");
		granularityDurationMap.put(Granularity.fifteen_minute, "PT15M");
		granularityDurationMap.put(Granularity.thirty_minute, "PT30M");
		granularityDurationMap.put(Granularity.hour, "PT1H");
		granularityDurationMap.put(Granularity.day, "P1D");
		granularityDurationMap.put(Granularity.week, "P1W");
		granularityDurationMap.put(Granularity.month, "P1M");
	}

	public static Granularity getGranularity(String granularityStr) {
		if(granularityStr != null){
			return mapGranularity.get(granularityStr.toLowerCase());
		}else{
			return Granularity.all;
		}
	}
	
	public static String getGranularityDuration(Granularity granularity) {
		return granularityDurationMap.get(granularity);
	}
	
	public static String getGranularityDuration(String granularityStr) {
		return granularityDurationMap.get(getGranularity(granularityStr));
	}

	private static Map<String, AggregateFunction> mapAggregateFunctions = EnumUtils.getEnumMap(AggregateFunction.class);
	static {
		// Use the following count(*) for countall
		mapAggregateFunctions.remove("countall");
		String countall = "count"+ '(' + '*' + ')';
		mapAggregateFunctions.put(countall, AggregateFunction.countall);
	}

	public static AggregateFunction getAggregateFunction(String function) {
		return mapAggregateFunctions.get(function);
	}


}
