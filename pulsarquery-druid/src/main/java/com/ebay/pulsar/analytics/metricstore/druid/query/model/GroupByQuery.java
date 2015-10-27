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
import com.ebay.pulsar.analytics.metricstore.druid.having.BaseHaving;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.DefaultLimitSpec;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * 
 * @author rtao
 *
 */
public class GroupByQuery extends BaseQuery {
	private static final Logger logger = LoggerFactory.getLogger(GroupByQuery.class);

	private List<String> dimensions;
	private DefaultLimitSpec limitSpec;
	private BaseHaving having;

	private static final byte GROUPBY_QUERY = 0x14;

	public GroupByQuery(String dataSource, List<String> intervals, BaseGranularity granularity,
			List<BaseAggregator> aggregations, List<String> dimensions) {
		super(QueryType.groupBy, dataSource, intervals, granularity, aggregations);
		this.dimensions = dimensions;
	}

	public List<String> getDimensions() {
		return dimensions;
	}

	public DefaultLimitSpec getLimitSpec() {
		return limitSpec;
	}

	public void setLimitSpec(DefaultLimitSpec limitSpec) {
		this.limitSpec = limitSpec;
	}

	public BaseHaving getHaving() {
		return having;
	}

	public void setHaving(BaseHaving having) {
		this.having = having;
	}

	@Override
	public byte[] cacheKey() {
		BaseFilter filter = this.getFilter();
		byte[] filterBytes = filter == null ? new byte[] {} : filter.cacheKey();
		byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(this.getAggregations());
		byte[] granularityBytes = null;
		if (this.getGranularity() instanceof BaseGranularity) {
			granularityBytes = ((BaseGranularity) this.getGranularity()).cacheKey();
		} else {
			granularityBytes = new byte[0];
		}
		byte[] intervalsBytes = QueryCacheHelper.computeIntervalsBytes(this.getIntervals());
		final byte[][] dimensionsBytes = new byte[this.getDimensions().size()][];
		int dimensionsBytesSize = 0;
		int index = 0;
		for (String dimension : this.getDimensions()) {
			dimensionsBytes[index] = dimension.getBytes();
			dimensionsBytesSize += dimensionsBytes[index].length;
			++index;
		}
		final byte[] limitBytes = this.getLimitSpec().cacheKey();

		BaseHaving having = this.getHaving();
		byte[] havingBytes = having == null ? new byte[] {} : having.cacheKey();
		
		List<BasePostAggregator> postaggregators = this.getPostAggregations();
		byte[] postaggregatorBytes = postaggregators == null ? new byte[] {} : QueryCacheHelper.computePostAggregatorBytes(postaggregators);

		ByteBuffer buffer = ByteBuffer
				.allocate(
						1 + granularityBytes.length + filterBytes.length + aggregatorBytes.length + dimensionsBytesSize
								+ limitBytes.length + havingBytes.length + intervalsBytes.length + postaggregatorBytes.length).put(GROUPBY_QUERY)
				.put(granularityBytes).put(filterBytes).put(aggregatorBytes).put(postaggregatorBytes);

		for (byte[] dimensionsByte : dimensionsBytes) {
			buffer.put(dimensionsByte);
		}
		return buffer.put(limitBytes).put(havingBytes).put(intervalsBytes).array();
	}
	
	@Override
	public String toString() {
		String query = "";
		ObjectWriter statswriter = new ObjectMapper().writerWithType(GroupByQuery.class);
		try {
			query = statswriter.writeValueAsString(this);
		} catch (IOException e) {
			logger.warn (e.getMessage());;
		}
		return query;
	}
}
