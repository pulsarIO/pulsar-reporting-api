/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ebay.pulsar.analytics.datasource.DataSourceProvider;
import com.ebay.pulsar.analytics.datasource.PulsarTableDimension;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.TableDimension;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.having.BaseHaving;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.DefaultLimitSpec;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.OrderByColumnSpec;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.DruidSpecs;
import com.foundationdb.sql.parser.AggregateNode;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.SelectNode;
import com.foundationdb.sql.parser.ValueNode;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidSQLTranslator extends AbstractDruidSqlTranslator {	
	private DruidFilterTranslator filterTranslator = new DruidFilterTranslator();
	private DruidHavingTranslator havingTranslator = new DruidHavingTranslator();
	private DruidAggregatorTranslator aggregatorTranslator = new DruidAggregatorTranslator();
	private DruidPostAggregatorTranslator postAggregatorTranslator = new DruidPostAggregatorTranslator();
	private DruidOrderByTranslator orderByTranslator = new DruidOrderByTranslator();

	// //////////////////////////////////////////////////////////////////////////////
	// This is used to parse SQL statement for Druid backend.
	// The results of the parsing are Druid Aggregator, PostAggregators, & etc.
	// //////////////////////////////////////////////////////////////////////////////
	public DruidSpecs parseSql(String sql, DataSourceProvider db,
			String tableName) throws SqlTranslationException {

		// sql = full "SELECT * FROM TBL WHERE "
		QueryDescription queryDesc = parse(sql);

		// SelectNode should have all the ResultColumnList, FromTable,
		// WhereClause, GroupByList, HavingClause, & OrderByList
		SelectNode selectNode = queryDesc.getSelectNode();

		final Table tableMeta = db.getTableByName(tableName);
		if (tableMeta == null) {
			throw new SqlTranslationException(
					ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
							+ "Invalid table name.");
		}

		ResultInfo parseInfo = parseResultList(selectNode, tableMeta);

		Map<String, AggregateNode> aggregateColsMap = parseInfo.getSimpleAggregateColsMap();
		Map<String, ValueNode> postAggregatesMap = parseInfo
				.getPostAggregatesMap();
		Map<String, String> aggrKeyToAliasMap = parseInfo
				.getAggrKeyToAliasMap();
		
		List<String> dimensions = parseInfo.getDimensions();
		
		//Dimension name to Alias or real dimension name map
		final Map<String, String> columnsMap = parseInfo.getColumnsMap();
		final Map<String, String> dimensionsToAliasMap = parseInfo.getDimensionsToAliasMap();

		List<BaseAggregator> aggregators = null;
		List<BasePostAggregator> postAggregators = null;
		String firstAlias = parseInfo.getFirstAlias();
		if (firstAlias == null) {
			throw new SqlTranslationException(
					ExceptionErrorCode.INVALID_AGGREGATE.getErrorMessage()
							+ "One Aggregate or PostAggregate required");
		}
		if (!Collections.disjoint(aggrKeyToAliasMap.values(),dimensions)) {
			throw new SqlTranslationException(
					ExceptionErrorCode.INVALID_METRIC.getErrorMessage()
							+ "Invalid Metic Alias");
		}
		if (aggregateColsMap.size() > 0) {
			aggregators = aggregatorTranslator.valueNodesToAggregators(aggregateColsMap, tableMeta);
		}
		if (postAggregatesMap.size() > 0) {
			postAggregators = postAggregatorTranslator.valueNodesToPostAggregators(postAggregatesMap,
					aggrKeyToAliasMap, tableMeta);
		}else{
			postAggregators = Lists.newArrayList();
		}

		BaseFilter filter = null;
		if (selectNode.getWhereClause() != null) {
			filter = filterTranslator.valueNodeToFilter(selectNode.getWhereClause(), tableMeta);
		}

		BaseHaving having = null;
		if (selectNode.getHavingClause() != null) {
			having = havingTranslator.valueNodeToHaving(selectNode.getHavingClause(), aggrKeyToAliasMap, postAggregators, tableMeta);
		}

		CursorNode cursorNode = queryDesc.getCursorNode();
		// Un-document the way to get LIMIT
		// This is from the
		// https://groups.google.com/a/akiban.com/forum/#!topic/akiban-user/JBjQ78kM3mk
		ValueNode limitNode = cursorNode.getFetchFirstClause();
		int limit = getIntValue(limitNode, DEFAULT_LIMIT);
		ValueNode offsetNode = cursorNode.getOffsetClause();
		int offset = getIntValue(offsetNode, 0);

		groupByCheck(selectNode, cursorNode, columnsMap);

		List<OrderByColumnSpec> orderSpec = orderByTranslator.cursorNodeToOrderBy(cursorNode,tableMeta, aggregators, aggrKeyToAliasMap, firstAlias);
		DefaultLimitSpec defaultLimitSpec = new DefaultLimitSpec(limit,
				orderSpec);
		if (postAggregators.size() == 0) {
			postAggregators = null;
		}
		DruidSpecs specs = null;
		if (dimensions != null) {
			//Real Dimension name to Alias map
			final Map<String, String> nameAliasMap = Maps.newHashMap();
			Set<String> transformedDimensions = FluentIterable
					.from(dimensions).transform(new Function<String, String>() {
						@Override
						public String apply(String input) {
							TableDimension tableDim = tableMeta
									.getDimensionByName(input);
							if (tableDim instanceof PulsarTableDimension) {
								PulsarTableDimension pulsarTableDimMeta = (PulsarTableDimension) tableDim;
								if (pulsarTableDimMeta != null && pulsarTableDimMeta.getRTOLAPColumnName() != null) {
									if (!pulsarTableDimMeta.getRTOLAPColumnName().equals(columnsMap.get(input))){
										nameAliasMap.put(pulsarTableDimMeta.getRTOLAPColumnName(), columnsMap.get(input));
										if(dimensionsToAliasMap.get(columnsMap.get(input))!=null){
											nameAliasMap.put(pulsarTableDimMeta.getRTOLAPColumnName(), dimensionsToAliasMap.get(columnsMap.get(input)));
										}
									}
									else if(!pulsarTableDimMeta.getRTOLAPColumnName().equals(input)){
										nameAliasMap.put(pulsarTableDimMeta.getRTOLAPColumnName(), input);	
								        if(dimensionsToAliasMap.get(input)!=null){
								        	nameAliasMap.put(pulsarTableDimMeta.getRTOLAPColumnName(), dimensionsToAliasMap.get(input));
										}
									}
									
									if (!pulsarTableDimMeta.getRTOLAPColumnName().equals(input)) {
										return pulsarTableDimMeta.getRTOLAPColumnName();
									}
								}
							}else{
								if(!input.equals(columnsMap.get(input))){
									nameAliasMap.put(input, columnsMap.get(input));
								}
							}
							return input;
						}
					}).toSet();
			specs = new DruidSpecs(tableName, new ArrayList<String>(transformedDimensions),
					nameAliasMap, aggregators, postAggregators);
		} else {
			specs = new DruidSpecs(tableName, null, null, aggregators,
					postAggregators);
		}

		// No Need for groupBy
		// specs.setSort ( );
		specs.setFilter(filter)
		.setHaving(having)
		.setLimit(limit)
		.setLimitSpec(defaultLimitSpec)
		.setTableColumnsMeta(tableMeta)
		.setOffset(offset);

		return specs;
	}
}
