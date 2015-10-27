/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.model;

import java.util.List;
import java.util.Map;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.having.BaseHaving;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.DefaultLimitSpec;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.OrderByColumnSpec;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.query.request.DateRange;

/**
 * 
 * @author rtao
 *
 */
public class DruidSpecs {
	private final String fromTable;
	private final List<String> dimensions;
	private final Map<String, String> nameAliasMap;
	private final List<BaseAggregator> aggregators;
	private final List<BasePostAggregator> postAggregators;
	private BaseFilter filter;
	private BaseHaving having;
	
	private int limit;
	private DefaultLimitSpec limitSpec;
	private String sort;
	private int offset;
	private Table tableColumnsMeta;
	private DateRange intervals;
	private BaseGranularity granularity;

	public DruidSpecs (String fromTable, List<String> dimensions, Map<String, String> nameAliasMap,  List<BaseAggregator> aggregators, List<BasePostAggregator> postAggregators) {
		this.fromTable = fromTable;
		this.dimensions = dimensions;
		this.nameAliasMap = nameAliasMap;
		this.aggregators = aggregators;
		this.postAggregators = postAggregators;
	}

	public Map<String, String> getNameAliasMap() {
		return nameAliasMap;
	}

	public String getFromTable() {
		return this.fromTable;
	}

	public List<String> getDimensions() {
		return this.dimensions;
	}

	public List<BaseAggregator> getAggregators() {
		return this.aggregators;
	}

	public List<BasePostAggregator> getPostAggregators() {
		return this.postAggregators;
	}

	public BaseFilter getFilter() {
		return this.filter;
	}

	public DruidSpecs setFilter(BaseFilter filter) {
		this.filter = filter;
		return this;
	}

	public BaseHaving getHaving() {
		return this.having;
	}

	public DruidSpecs setHaving(BaseHaving having) {
		this.having = having;
		return this;
	}

	public DefaultLimitSpec getLimitSpec () {
		return this.limitSpec;
	}
	
	public DruidSpecs setLimitSpec (DefaultLimitSpec limitSpec) {
		this.limitSpec = limitSpec;
		return this;
	}
	
	public DruidSpecs setSort(String sort){
		this.sort = sort;
		return this;
	}
	
	public String getSort(){
		String sort = null;
		if (this.limitSpec != null) {
			List<OrderByColumnSpec> orderByCols = limitSpec.getColumns();
			if (orderByCols != null && orderByCols.size() > 0) {
				OrderByColumnSpec colSpec = orderByCols.get(0);
				sort = colSpec.getDimension();
			}
		}else{
			sort = this.sort;
		}
		return sort;
	}

	public int getLimit() {
		return this.limit;
	}

	public DruidSpecs setLimit (int limit) {
		this.limit = limit;
		return this;
	}
	
	public int getOffset() {
		return this.offset;
	}

	public DruidSpecs setOffset (int offset) {
		this.offset = offset;
		return this;
	}

	public Table getTableColumnsMeta() {
		return this.tableColumnsMeta;
	}

	public DruidSpecs setTableColumnsMeta (Table tableColumnsMeta) {
		this.tableColumnsMeta = tableColumnsMeta;
		return this;
	}
	
	public DateRange getIntervals() {
		return intervals;
	}

	public DruidSpecs setIntervals(DateRange intervals) {
		this.intervals = intervals;
		return this;
	}

	public BaseGranularity getGranularity() {
		return granularity;
	}

	public DruidSpecs setGranularity(BaseGranularity granularity) {
		this.granularity = granularity;
		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((aggregators == null) ? 0 : aggregators.hashCode());
		result = prime * result
				+ ((dimensions == null) ? 0 : dimensions.hashCode());
		result = prime * result + ((filter == null) ? 0 : filter.hashCode());
		result = prime * result
				+ ((fromTable == null) ? 0 : fromTable.hashCode());
		result = prime * result
				+ ((granularity == null) ? 0 : granularity.hashCode());
		result = prime * result + ((having == null) ? 0 : having.hashCode());
		result = prime * result
				+ ((intervals == null) ? 0 : intervals.hashCode());
		result = prime * result + limit;
		result = prime * result
				+ ((limitSpec == null) ? 0 : limitSpec.hashCode());
		result = prime * result
				+ ((nameAliasMap == null) ? 0 : nameAliasMap.hashCode());
		result = prime * result + offset;
		result = prime * result
				+ ((postAggregators == null) ? 0 : postAggregators.hashCode());
		result = prime * result + ((sort == null) ? 0 : sort.hashCode());
		result = prime
				* result
				+ ((tableColumnsMeta == null) ? 0 : tableColumnsMeta.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DruidSpecs other = (DruidSpecs) obj;
		if (aggregators == null) {
			if (other.aggregators != null)
				return false;
		} else if (!aggregators.equals(other.aggregators))
			return false;
		if (dimensions == null) {
			if (other.dimensions != null)
				return false;
		} else if (!dimensions.equals(other.dimensions))
			return false;
		if (filter == null) {
			if (other.filter != null)
				return false;
		} else if (!filter.equals(other.filter))
			return false;
		if (fromTable == null) {
			if (other.fromTable != null)
				return false;
		} else if (!fromTable.equals(other.fromTable))
			return false;
		if (granularity == null) {
			if (other.granularity != null)
				return false;
		} else if (!granularity.equals(other.granularity))
			return false;
		if (having == null) {
			if (other.having != null)
				return false;
		} else if (!having.equals(other.having))
			return false;
		if (intervals == null) {
			if (other.intervals != null)
				return false;
		} else if (!intervals.equals(other.intervals))
			return false;
		if (limit != other.limit)
			return false;
		if (limitSpec == null) {
			if (other.limitSpec != null)
				return false;
		} else if (!limitSpec.equals(other.limitSpec))
			return false;
		if (offset != other.offset)
			return false;
		if (sort == null) {
			if (other.sort != null)
				return false;
		} else if (!sort.equals(other.sort))
			return false;
		if (tableColumnsMeta == null) {
			if (other.tableColumnsMeta != null)
				return false;
		} else if (!tableColumnsMeta.equals(other.tableColumnsMeta))
			return false;
		if (nameAliasMap == null) {
			if (other.nameAliasMap != null)
				return false;
		} else if (!nameAliasMap.equals(other.nameAliasMap))
			return false;
		if (postAggregators == null) {
			if (other.postAggregators != null)
				return false;
		} else if (!postAggregators.equals(other.postAggregators))
			return false;
		return true;
	}
}
