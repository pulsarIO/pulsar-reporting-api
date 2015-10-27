/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Period;

import com.ebay.pulsar.analytics.query.request.DateRange;
import com.ebay.pulsar.analytics.query.request.PulsarDateTimeFormatter;
import com.google.common.base.Strings;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarDataSourceRoutingStrategy implements DataSourceRoutingStrategy {

	private Map<String ,PulsarDataSourceRule> configuration;
	
	public Map<String, PulsarDataSourceRule> getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Map<String, PulsarDataSourceRule> configuration) {
		this.configuration = configuration;
	}

	@Override
	public String getDataSourceName(String tableName, DateRange range, String molapName, String rtolapName) {
		PulsarDataSourceRule rule = configuration.get(tableName);
		if(!Strings.isNullOrEmpty(molapName) && rule != null){
			Period period = PulsarDateTimeFormatter.ISO_PERIOD_FORMATTER.parsePeriod(rule.getPeriod());
			DateTime start = range.getStart();	
			DateTime now = new DateTime(PulsarDateTimeFormatter.MST_TIMEZONE);
			DateTime closeDate = now.minus(period);
			//MOLAP
			if(start.isBefore(closeDate)){
				return molapName;
			}else{
				return rtolapName;
			}
		}
		else 
			return rtolapName;
	}
}
