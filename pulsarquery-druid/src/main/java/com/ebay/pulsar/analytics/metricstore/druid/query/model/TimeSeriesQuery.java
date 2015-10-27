/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.QueryType;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * 
 * @author rtao
 *
 */
public class TimeSeriesQuery extends BaseQuery {
	private static final Logger logger = LoggerFactory.getLogger(TimeSeriesQuery.class);

	private static final byte TIMESERIES_QUERY = 0x0;

	public TimeSeriesQuery(String dataSource, List<String> intervals, BaseGranularity granularity,
			List<BaseAggregator> aggregations) {
		super(QueryType.timeseries, dataSource, intervals, granularity, aggregations);
	}

	public byte[] cacheKey() {
		BaseFilter filter = this.getFilter();
		byte[] filterBytes = filter == null ? new byte[] {} : filter.cacheKey();
		byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(this.getAggregations());
		byte[] intervalsBytes = QueryCacheHelper.computeIntervalsBytes(this.getIntervals());

		List<BasePostAggregator> postaggregators = this.getPostAggregations();
		byte[] postaggregatorBytes = postaggregators == null ? new byte[] {} : QueryCacheHelper
				.computePostAggregatorBytes(postaggregators);
		if (this.getGranularity() instanceof BaseGranularity) {
			byte[] granularityBytes = ((BaseGranularity) this.getGranularity()).cacheKey();
			return ByteBuffer
					.allocate(
							1 + granularityBytes.length + filterBytes.length + aggregatorBytes.length
									+ intervalsBytes.length + postaggregatorBytes.length).put(TIMESERIES_QUERY)
					.put(granularityBytes).put(filterBytes).put(aggregatorBytes).put(intervalsBytes)
					.put(postaggregatorBytes).array();
		}
		return null;
	}

	@Override
	public String toString() {
		String query = "";
		ObjectWriter statswriter = new ObjectMapper().writerWithType(TimeSeriesQuery.class);
		try {
			query = statswriter.writeValueAsString(this);
		} catch (IOException e) {
			logger.warn("TimeSeriesQuery:" + e.getMessage());
		}
		return query;
	}
}
