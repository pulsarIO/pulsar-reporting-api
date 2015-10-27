/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.constants;

/**
 * 
 * @author rtao
 *
 */
public final class Constants {
	public enum QueryType {
		timeseries,
		topN,
		groupBy,
		search;
	}
	
	public enum AggregatorType {
		count,
		longSum,
		doubleSum,
		min,
		max,
		cardinality,
		hyperUnique,
		filtered;
	}
	
	public enum PostAggregatorType {
		arithmetic,
		fieldAccess,
		constant,
		hyperUniqueCardinality
	}
	
	public enum FilterType {
		selector,
		regex,
		and,
		or,
		not
	}
	
	public enum MetricType {
		numeric,
		lexicographic,
		alphaNumeric,
		inverted
	}
	
	public enum SortDirection {
		ascending,
		descending
	}
	
	public enum HavingType {
		equalTo,
		greaterThan,
		lessThan,
		and,
		or,
		not
	}
}
