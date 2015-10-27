/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.granularity;

import com.ebay.pulsar.analytics.constants.Constants.Granularity;
import com.ebay.pulsar.analytics.constants.ConstantsUtils;
import com.ebay.pulsar.analytics.query.request.PulsarDateTimeFormatter;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidGranularityParser {	
	private static final String DEFAULT_ORIGIN1 = "1970-01-04T00:00:00-07:00";
	private static final String DEFAULT_ORIGIN2 = "1970-01-01T00:00:00-07:00";
	
	public static BaseGranularity parse(String granularityStr) {
		if(granularityStr == null || granularityStr.equalsIgnoreCase("all")) {
			return BaseGranularity.ALL;
		}
		String origin = DEFAULT_ORIGIN1;
		String duration = null;
		Granularity granluarityEnum = ConstantsUtils.getGranularity(granularityStr);
		if (granluarityEnum != null) {
			duration = ConstantsUtils.getGranularityDuration(granluarityEnum);
			if(granluarityEnum == Granularity.month){
				origin = DEFAULT_ORIGIN2;
			}
		} else {
			try {
				PulsarDateTimeFormatter.ISO_PERIOD_FORMATTER.parsePeriod(granularityStr);
			} catch (Exception ex) {
				return new InvalidGranularity(granularityStr);
			}
			// passed the newDuration() function, that means it's a valid iso8601 string
			duration = granularityStr;
		}
		return new PeriodGranularity(duration, "MST", origin);
	}
}
