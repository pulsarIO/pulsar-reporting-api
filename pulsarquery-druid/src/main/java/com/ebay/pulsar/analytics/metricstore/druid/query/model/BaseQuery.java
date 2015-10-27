/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.model;

import java.util.List;

import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.QueryType;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.google.common.collect.Lists;

/**
 * 
 * @author rtao
 *
 */
public abstract class BaseQuery {
	private QueryType queryType;
	private String dataSource;
	private List<String> intervals;
	private BaseGranularity granularity;
	private List<BaseAggregator> aggregations = Lists.newArrayList();
	private BaseFilter filter;
	private List<BasePostAggregator> postAggregations;
	
	public BaseQuery(QueryType queryType, String dataSource, List<String> intervals, BaseGranularity granularity, List<BaseAggregator> aggregations) {
		this.queryType = queryType;
		this.dataSource = dataSource;
		this.intervals = intervals;
		this.granularity = granularity;
		this.aggregations = aggregations;
	}
	
	public QueryType getQueryType() {
		return queryType;
	}
	
	public String getDataSource() {
		return dataSource;
	}

	public List<String> getIntervals() {
		return intervals;
	}

	public BaseGranularity getGranularity() {
		return granularity;
	}

	public List<BaseAggregator> getAggregations() {
		return aggregations;
	}

	public BaseFilter getFilter() {
		return filter;
	}

	public void setGranularity (BaseGranularity granularity) {
		this.granularity = granularity;
	}

	public void setFilter(BaseFilter filter) {
		this.filter = filter;
	}

	public List<BasePostAggregator> getPostAggregations() {
		return postAggregations;
	}

	public void setPostAggregations(List<BasePostAggregator> postAggregations) {
		this.postAggregations = postAggregations;
	}
	
	public abstract byte[] cacheKey();
}
