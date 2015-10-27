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
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.ArithmeticPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.ConstantPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.FieldAccessorPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.HyperUniqueCardinalityPostAggregator;
import com.foundationdb.sql.parser.AggregateNode;
import com.foundationdb.sql.parser.BinaryArithmeticOperatorNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.NumericConstantNode;
import com.foundationdb.sql.parser.ValueNode;
import com.google.common.collect.Lists;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidPostAggregatorTranslator extends AbstractDruidSqlTranslator{
	private static final Logger logger = LoggerFactory.getLogger(DruidPostAggregatorTranslator.class);
	
	public List<BasePostAggregator> valueNodesToPostAggregators(
			Map<String, ValueNode> postAggregatesMap,
			Map<String, String> aggrKeyToAliasMap, Table tableColumnsMeta)
			throws SqlTranslationException {
		List<BasePostAggregator> postAggregators = Lists.newArrayList();

		for (Map.Entry<String, ValueNode> entry : postAggregatesMap.entrySet()) {
			String alias = entry.getKey();
			ValueNode vNode = postAggregatesMap.get(alias);
			BasePostAggregator postAggregator = valueNodeToPostAggregator(
					vNode, alias, aggrKeyToAliasMap, tableColumnsMeta);
			postAggregators.add(postAggregator);
		}
		return postAggregators;
	}

	private BasePostAggregator valueNodeToPostAggregator(ValueNode vNode,
			String alias, Map<String, String> aggrKeyToAliasMap,
			Table tableColumnsMeta) throws SqlTranslationException {
		BasePostAggregator postAggregator = null;
		if (vNode instanceof BinaryArithmeticOperatorNode) {
			BinaryArithmeticOperatorNode bNode = (BinaryArithmeticOperatorNode) vNode;
			String operator = bNode.getOperator();
			ValueNode lNode = bNode.getLeftOperand();
			BasePostAggregator postAggr1 = valueNodeToPostAggregator(lNode,
					null, aggrKeyToAliasMap, tableColumnsMeta);

			ValueNode rNode = bNode.getRightOperand();
			BasePostAggregator postAggr2 = valueNodeToPostAggregator(rNode,
					null, aggrKeyToAliasMap, tableColumnsMeta);

			// Auto-generated Name
			if (alias == null) {
				alias = arithFuncMap.get(operator) + "_" + postAggr1.getName()
						+ "_" + postAggr2.getName();
			}
			postAggregator = new ArithmeticPostAggregator(alias, operator,
					postAggr1, postAggr2);
			if(logger.isDebugEnabled()){
				logger.debug("Operator: " + operator);
			}
		} else if (vNode instanceof AggregateNode) {
			AggregateNode aNode = (AggregateNode) vNode;
			String key = this.getAggregateKey(aNode,tableColumnsMeta, null, false);
			String name = aggrKeyToAliasMap.get(key);
			if (name != null) {
				if(aNode.isDistinct()){
					postAggregator = new HyperUniqueCardinalityPostAggregator(name, name);
				}
				else{
					postAggregator = new FieldAccessorPostAggregator(name, name);
				}
			} else {
				throw new SqlTranslationException(
						ExceptionErrorCode.INVALID_AGGREGATE.getErrorMessage()
								+ "Aggregate required for Post Aggregate: "
								+ key);
			}
		} else if (vNode instanceof ColumnReference) {
			ColumnReference column = (ColumnReference) vNode;
			String colName = column.getColumnName();
			if(logger.isDebugEnabled()){
				logger.debug("Column: " + colName);
			}
		} else if (vNode instanceof NumericConstantNode) {
			NumericConstantNode nNode = (NumericConstantNode) vNode;
			Object valueObj = nNode.getValue();
			Number value = (Number) valueObj;
			String name = "Const" + value;
			postAggregator = new ConstantPostAggregator(name, value);
		} else {
			logger.info("Other type: " + vNode.getType());
		}
		return postAggregator;
	}
}
