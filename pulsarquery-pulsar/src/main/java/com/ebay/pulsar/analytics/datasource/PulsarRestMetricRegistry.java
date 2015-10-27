/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarRestMetricRegistry {
	private List<PulsarRestMetricMeta> metricsList;
	private Map<String, PulsarRestMetricMeta> metricsMap = Maps.newHashMap();
	
	public List<PulsarRestMetricMeta> getMetricsList() {
		return metricsList;
	}
	public void setMetricsList(List<PulsarRestMetricMeta> metricsList) {
		this.metricsList = metricsList;
	}
	
	public PulsarRestMetricRegistry (List<PulsarRestMetricMeta> metricsList) {
		this.metricsList = metricsList;
		for (PulsarRestMetricMeta metricMeta : metricsList) {
			String metricName = metricMeta.getMetricName();
			metricsMap.put(metricName, metricMeta);

			List<String> kylinAggregates = metricMeta.getKylinAggregators();
			if (kylinAggregates == null) {
				continue;
			}
			Map<String, String> aliasAggregateMap = Maps.newHashMap();
			for (String aggregate : kylinAggregates) {
				int idx = aggregate.lastIndexOf(' ');
				if (idx > 0) {
					String alias = aggregate.substring(idx+1);
					String aggrSql = aggregate.substring(0, idx);
					aliasAggregateMap.put(alias.toLowerCase(), aggrSql);
				}
			}
			metricMeta.setKylinAliasAggregatorMap(aliasAggregateMap);
		}
	}
	
	public PulsarRestMetricMeta getMetricsMetaFromName (String metricName) {
		return metricsMap.get(metricName);
	}

}
