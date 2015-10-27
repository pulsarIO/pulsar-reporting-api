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
import com.ebay.pulsar.analytics.metricstore.druid.metric.BaseMetric;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.primitives.Ints;

/**
 * 
 * @author rtao
 *
 */
public class TopNQuery extends BaseQuery {
	private static final Logger logger = LoggerFactory.getLogger(TopNQuery.class);

	private String dimension;
	private int threshold;
	private BaseMetric metric;

	private static final byte TOPN_QUERY = 0x1;

	public TopNQuery(String dataSource, List<String> intervals, BaseGranularity granularity,
			List<BaseAggregator> aggregations, String dimension, int threshold, BaseMetric metric) {
		super(QueryType.topN, dataSource, intervals, granularity, aggregations);
		this.dimension = dimension;
		this.threshold = threshold;
		this.metric = metric;
	}

	public String getDimension() {
		return dimension;
	}

	public int getThreshold() {
		return threshold;
	}

	public BaseMetric getMetric() {
		return metric;
	}

	@Override
	public byte[] cacheKey() {
		byte[] dimensionSpecBytes = this.getDimension().getBytes();
		byte[] metricSpecBytes = this.getMetric().cacheKey();

		BaseFilter filter = this.getFilter();
		byte[] filterBytes = filter == null ? new byte[] {} : filter.cacheKey();
		byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(this.getAggregations());
		byte[] granularityBytes = null;

		if(this.getGranularity() instanceof BaseGranularity) {
			granularityBytes = ((BaseGranularity) this.getGranularity()).cacheKey();
		} else {
			granularityBytes = new byte[0];
		}

		byte[] intervalsBytes = QueryCacheHelper.computeIntervalsBytes(this.getIntervals());

		List<BasePostAggregator> postaggregators = this.getPostAggregations();
		byte[] postaggregatorBytes = postaggregators == null ? new byte[] {} : QueryCacheHelper
				.computePostAggregatorBytes(postaggregators);

		return ByteBuffer
				.allocate(
						1 + dimensionSpecBytes.length + metricSpecBytes.length + 4 + granularityBytes.length
								+ filterBytes.length + aggregatorBytes.length + intervalsBytes.length
								+ postaggregatorBytes.length).put(TOPN_QUERY).put(dimensionSpecBytes)
				.put(metricSpecBytes).put(Ints.toByteArray(this.getThreshold())).put(granularityBytes).put(filterBytes)
				.put(aggregatorBytes).put(intervalsBytes).put(postaggregatorBytes).array();
	}

	@Override
	public String toString() {
		String query = "";
		ObjectWriter statswriter = new ObjectMapper().writerWithType(TopNQuery.class);
		try {
			query = statswriter.writeValueAsString(this);
		} catch (IOException e) {
			logger.warn("TopNQuery:" + e.getMessage());
		}
		return query;
	}
}
