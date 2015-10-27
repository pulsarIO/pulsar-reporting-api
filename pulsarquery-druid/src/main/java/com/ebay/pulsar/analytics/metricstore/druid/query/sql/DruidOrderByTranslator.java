/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.sql;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.SortDirection;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.OrderByColumnSpec;
import com.foundationdb.sql.parser.AggregateNode;
import com.foundationdb.sql.parser.BinaryArithmeticOperatorNode;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.OrderByColumn;
import com.foundationdb.sql.parser.OrderByList;
import com.foundationdb.sql.parser.ValueNode;
import com.google.common.collect.Lists;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidOrderByTranslator extends AbstractDruidSqlTranslator {
	private static final Logger logger = LoggerFactory.getLogger(DruidPostAggregatorTranslator.class);
	
	public List<OrderByColumnSpec> cursorNodeToOrderBy(CursorNode cursorNode,
			Table tableColumnsMeta, List<BaseAggregator> aggregators,
			Map<String, String> aggrKeyToAliasMap, String firstAlias)
			throws SqlTranslationException {
		OrderByList orderByList = cursorNode.getOrderByList();
		List<OrderByColumnSpec> orderSpec = Lists.newArrayList();
		ValueNode vNode = null;
		if (orderByList == null || orderByList.size() == 0) {
			// If no "ORDER BY" then use the first aggregate as "ORDER BY"
			// THEN the firstAlias (Aggregate or PostAggregate alias) is used

			OrderByColumnSpec orderByColumnSpec = new OrderByColumnSpec(
					firstAlias, SortDirection.descending);
			orderSpec.add(orderByColumnSpec);
			logger.info("Order by (first Alias): " + firstAlias);
		} else {
			int listSize = orderByList.size();
			for (int i = 0; i < listSize; i++) {
				OrderByColumn orderByCol = (OrderByColumn) orderByList.get(i);
				vNode = orderByCol.getExpression();

				boolean ascend = orderByCol.isAscending();
				SortDirection orderDirection = null;
				if (ascend) {
					orderDirection = SortDirection.ascending;
				} else {
					orderDirection = SortDirection.descending;
				}

				String key = null;
				if (vNode instanceof AggregateNode) {
					key = getAggregateKey((AggregateNode) vNode,
							tableColumnsMeta, null, true);
				} else if (vNode instanceof BinaryArithmeticOperatorNode) {
					// Same method to get Post Aggregate Key but no Column
					// Reference Checking
					key = addCompositeAggregateNode(vNode, null, aggrKeyToAliasMap, null,
							tableColumnsMeta, false);
				} else {
					throw new SqlTranslationException(
							ExceptionErrorCode.INVALID_SORT_PARAM
									.getErrorMessage()
									+ "Invalid Order By column: " + vNode.getColumnName());
				}

				String aliasName = aggrKeyToAliasMap.get(key);
				if (aliasName == null) {
					throw new SqlTranslationException(
							ExceptionErrorCode.INVALID_SORT_PARAM
									.getErrorMessage()
									+ "No aggregate column defined for Order By: "
									+ key);
				}
				OrderByColumnSpec orderByColumnSpec = new OrderByColumnSpec(
						aliasName, orderDirection);
				orderSpec.add(orderByColumnSpec);
				if(logger.isDebugEnabled()){
					logger.debug("Column: " + aliasName);
				}
			}
		}
		return orderSpec;
	}
}
