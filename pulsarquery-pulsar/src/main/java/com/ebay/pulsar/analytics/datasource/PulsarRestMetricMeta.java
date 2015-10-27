/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.having.BaseHaving;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarRestMetricMeta {

	private String metricName;

	private Set<String> metricEndpoints;
	private String tableName;
	
	private List<BaseAggregator> druidAggregators;
	private List<BasePostAggregator> druidPostAggregators;
	private BaseFilter druidFilter;
	private BaseHaving druidHaving;
	
	private List<String> kylinAggregators;
	private List<String> kylinPostAggregators;
	private Map<String, String> kylinAliasAggregatorMap;
	private String kylinFilter;
	private String kylinHaving;

	public String getMetricName () {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	public Set<String> getMetricEndpoints () {
		return metricEndpoints;
	}

	public void setMetricEndpoints(Set<String> metricEndpoints) {
		this.metricEndpoints = metricEndpoints;
	}

	public String getTableName () {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<BaseAggregator> getDruidAggregators () {
		return druidAggregators;
	}

	public void setDruidAggregators (List<BaseAggregator> druidAggregators) {
		this.druidAggregators = druidAggregators;
	}

	public List<BasePostAggregator> getDruidPostAggregators () {
		return druidPostAggregators;
	}

	public void setDruidPostAggregators (List<BasePostAggregator> druidPostAggregators) {
		this.druidPostAggregators = druidPostAggregators;
	}

	public BaseFilter getDruidFilter () {
		return druidFilter;
	}

	public void setDruidFilter (BaseFilter druidFilter) {
		this.druidFilter = druidFilter;
	}

	public BaseHaving getDruidHaving () {
		return druidHaving;
	}

	public void setDruidHaving (BaseHaving druidHaving) {
		this.druidHaving = druidHaving;
	}

	public List<String> getKylinAggregators () {
		return kylinAggregators;
	}

	public void setKylinAggregators (List<String> kylinAggregators) {
		this.kylinAggregators = kylinAggregators;
	}

	public List<String> getKylinPostAggregators () {
		return kylinPostAggregators;
	}

	public void setKylinPostAggregators (List<String> kylinPostAggregators) {
		this.kylinPostAggregators = kylinPostAggregators;
	}

	public Map<String, String> getKylinAliasAggregatorMap () {
		return kylinAliasAggregatorMap;
	}

	public void setKylinAliasAggregatorMap (Map<String, String> kylinAliasAggregatorMap) {
		this.kylinAliasAggregatorMap = kylinAliasAggregatorMap;
	}

	public String getKylinFilter () {
		return kylinFilter;
	}

	public void setKylinFilter (String kylinFilter) {
		this.kylinFilter = kylinFilter;
	}

	public String getKylinHaving () {
		return kylinHaving;
	}

	public void setKylinHaving (String kylinHaving) {
		this.kylinHaving = kylinHaving;
	}
}
