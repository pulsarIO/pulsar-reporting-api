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

import com.ebay.pulsar.analytics.constants.Constants.AggregateFunction;
import com.ebay.pulsar.analytics.constants.ConstantsUtils;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.CardinalityAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.DoubleSumAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.HyperUniqueAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.LongSumAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.MaxAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.MinAggregator;
import com.foundationdb.sql.parser.AggregateNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.NumericConstantNode;
import com.foundationdb.sql.parser.ValueNode;
import com.google.common.collect.Lists;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidAggregatorTranslator extends AbstractDruidSqlTranslator{
	private static final Logger logger = LoggerFactory.getLogger(DruidAggregatorTranslator.class);
		
	public List<BaseAggregator> valueNodesToAggregators(
			Map<String, AggregateNode> aggregateColsMap, Table tableColumnsMeta)
			throws SqlTranslationException {
		List<BaseAggregator> aggregators = Lists.newArrayList();
		for (Map.Entry<String, AggregateNode> entry : aggregateColsMap.entrySet()) {
			String alias = entry.getKey();
			AggregateNode aNode = entry.getValue();
			String funcName = aNode.getAggregateName().toLowerCase();
			ValueNode vNode = aNode.getOperand();

			BaseAggregator aggregator = null;
			AggregateFunction aggrFunc = ConstantsUtils
					.getAggregateFunction(funcName);

			if (aggrFunc != AggregateFunction.countall) {
				if (vNode == null) {
					throw new SqlTranslationException(
							ExceptionErrorCode.INVALID_AGGREGATE
									.getErrorMessage()
									+ "No column refernce found for Group By ");
				}
			}
			ColumnReference column = null;
			String columnName = null;

			// Check if column Double
			boolean isDouble = false;
			String fieldName = null;

			if (vNode != null) {
				if (vNode instanceof ColumnReference) {
					column = (ColumnReference) vNode;
					columnName = column.getColumnName();
					fieldName = columnRefCheck(column, tableColumnsMeta, null, !aNode.isDistinct(), tableColumnsMeta.isColumnHyperLogLog(columnName));
					isDouble = tableColumnsMeta.isColumnDouble(columnName);
				} else {
					if (vNode instanceof NumericConstantNode) {
						// This has to be count(1)
						aggrFunc = AggregateFunction.countall;
					}
				}
			}

			switch (aggrFunc) {
			case count:
				if (aNode.isDistinct()) {
					if (tableColumnsMeta.isColumnHyperLogLog(fieldName)) {
						aggregator = new HyperUniqueAggregator(alias, fieldName);
					} else {
						List<String> fieldNames = Lists.newArrayList();
						fieldNames.add(fieldName);
						aggregator = new CardinalityAggregator(alias,
								fieldNames);
					}
				} else {
					aggregator = new LongSumAggregator(alias, "count");
				}
				break;
			case countall:
				// count(*) & count(1)
				aggregator = new LongSumAggregator(alias, "count");
				break;
			case sum:
				if (isDouble) {
					aggregator = new DoubleSumAggregator(alias, fieldName);
				} else {
					aggregator = new LongSumAggregator(alias, fieldName);
				}
				break;
			case min:
				aggregator = new MinAggregator(alias, fieldName);
				break;
			case max:
				aggregator = new MaxAggregator(alias, fieldName);
			}
			aggregators.add(0, aggregator);
			if(logger.isDebugEnabled()){
				logger.debug("Aggregate: " + alias);
			}
		}
		return aggregators;
	}
}
