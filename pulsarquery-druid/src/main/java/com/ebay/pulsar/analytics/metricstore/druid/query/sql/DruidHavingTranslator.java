/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.sql;

import java.util.List;
import java.util.Map;

import com.ebay.pulsar.analytics.constants.Constants.AggregateFunction;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.metricstore.druid.having.AndHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.BaseHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.EqualToHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.GreaterThanHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.LessThanHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.NotHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.OrHaving;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.ArithmeticPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.ConstantPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.HyperUniqueCardinalityPostAggregator;
import com.foundationdb.sql.parser.AggregateNode;
import com.foundationdb.sql.parser.AndNode;
import com.foundationdb.sql.parser.BinaryRelationalOperatorNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.NotNode;
import com.foundationdb.sql.parser.OrNode;
import com.foundationdb.sql.parser.ValueNode;
import com.google.common.collect.Lists;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidHavingTranslator extends AbstractDruidSqlTranslator {
	private static ConstantPostAggregator CONSTZERO = new ConstantPostAggregator ("const_zero", 0);

	public BaseHaving valueNodeToHaving (ValueNode vNode, Map<String, String> aggregateNames, List<BasePostAggregator> postAggregators,
			Table tableColumnsMeta){
		BaseHaving having = null;
		int nodeType = vNode.getNodeType();

		if (vNode instanceof BinaryRelationalOperatorNode) {
			BinaryRelationalOperatorNode bNode = (BinaryRelationalOperatorNode) vNode;
			ValueNode lNode = bNode.getLeftOperand();
			ValueNode rNode = bNode.getRightOperand();
			String value = null;

			String key = null;
			String aliasName = null;
			
			AggregateNode aNode = null;
			if (lNode instanceof AggregateNode && rNode instanceof ConstantNode) {
				aNode = (AggregateNode) lNode;
				ConstantNode cNode = (ConstantNode) rNode;
				if(cNode.getValue() == null){
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
							+"Un-Support Binary Having");
				}
				value = cNode.getValue().toString();
			}else if (rNode instanceof AggregateNode && lNode instanceof ConstantNode) {
				aNode = (AggregateNode) rNode;
				ConstantNode cNode = (ConstantNode) lNode;
				if(cNode.getValue() == null){
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
							+"Un-Support Binary Having");
				}
				value = cNode.getValue().toString();
			}else if (lNode instanceof ColumnReference && rNode instanceof ConstantNode) {
				ColumnReference colRef = (ColumnReference) lNode;
				ConstantNode cNode = (ConstantNode) rNode;
				if(cNode.getValue() == null){
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
							+"Un-Support Binary Having");
				}
				value = cNode.getValue().toString();
				key = colRef.getColumnName();
				aliasName = colRef.getColumnName();
			}else if (rNode instanceof ColumnReference && lNode instanceof ConstantNode) {
				ColumnReference colRef = (ColumnReference) rNode;
				ConstantNode cNode = (ConstantNode) lNode;
				if(cNode.getValue() == null){
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
							+"Un-Support Binary Having");
				}
				value = cNode.getValue().toString();
				key = colRef.getColumnName();
				aliasName = colRef.getColumnName();
			}else {
				throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
						+ "Un-Support Having Column");
			}
			if(aNode != null){
				key = this.getAggregateKey(aNode, tableColumnsMeta, null, false);
				aliasName = aggregateNames.get(key);
				if (aliasName == null) {
					throw new SqlTranslationException(ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
							+"Aggregate Column not defined");
				}
			}
			
			boolean hllHaving = false;
			if (aNode != null) {
				String funcName = aNode.getAggregateName().toLowerCase();
				if (aNode.isDistinct() && AggregateFunction.count.name().equals(funcName)) {
					HyperUniqueCardinalityPostAggregator hllAggregator = new HyperUniqueCardinalityPostAggregator (key, aliasName);
					// Add new PostAggregator for HyperUnique Having
					String hllAlias = HllConstants.HLLPREFIX + aliasName;
					ArithmeticPostAggregator hllPostAggregator = new ArithmeticPostAggregator(hllAlias, "+", hllAggregator, CONSTZERO);
					postAggregators.add(hllPostAggregator);
					aliasName = hllAlias;
					hllHaving = true;
				}
			}else{
				String fieldName = aggregateNames.get(key);
				if(fieldName == null){
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
							+"Invalid Aggregate in Having: " + key);
				}
				if(tableColumnsMeta.isColumnHyperLogLog(fieldName)){
					hllHaving = true;
					HyperUniqueCardinalityPostAggregator hllAggregator = new HyperUniqueCardinalityPostAggregator (key, aliasName);
					// Add new PostAggregator for HyperUnique Having
					String hllAlias = HllConstants.HLLPREFIX + aliasName;
					ArithmeticPostAggregator hllPostAggregator = new ArithmeticPostAggregator(hllAlias, "+", hllAggregator, CONSTZERO);
					postAggregators.add(hllPostAggregator);
					aliasName = hllAlias;
				}
			}
			switch (nodeType) {
			case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
				if (hllHaving) {
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
							+"Un-Support Druid Binary HyperUnique Having Equal Condition");
				}
				having = new EqualToHaving (aliasName, value);
				break;
			case NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:
				if (hllHaving) {
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
							+"Un-Support Druid Binary HyperUnique Having Not Equal Condition");
				}
				BaseHaving havingEq = new EqualToHaving (aliasName, value);
				having = new NotHaving (havingEq);
				break;
			case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
				having = new GreaterThanHaving (aliasName, value);
				break;
			case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
				having = new LessThanHaving (aliasName, value);
				break;
			case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
				List<BaseHaving> havingList = Lists.newArrayList();
				havingList.add(new GreaterThanHaving (aliasName, value));
				havingList.add(new EqualToHaving (aliasName, value));
				having = new OrHaving(havingList);
				break;
			case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
				havingList = Lists.newArrayList();
				havingList.add(new LessThanHaving (aliasName, value));
				havingList.add(new EqualToHaving (aliasName, value));
				having = new OrHaving(havingList);
				break;
			default:
				throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
						+"Un-Support Binary Having");
			}
		} else {
			switch (nodeType) {
			case NodeTypes.AND_NODE: {
				AndNode andNode = (AndNode) vNode;
				ValueNode lNode = andNode.getLeftOperand();
				ValueNode rNode = andNode.getRightOperand();
				BaseHaving lHaving = valueNodeToHaving (lNode, aggregateNames,postAggregators, tableColumnsMeta);
				BaseHaving rHaving = valueNodeToHaving (rNode, aggregateNames,postAggregators, tableColumnsMeta);
				List<BaseHaving> havingList = Lists.newArrayList();
				if (lHaving instanceof AndHaving) {
					havingList.addAll (((AndHaving)lHaving).getHavingSpecs());
				} else {
					havingList.add (lHaving);
				}
				if (rHaving instanceof AndHaving) {
					havingList.addAll (((AndHaving)rHaving).getHavingSpecs());
				} else {
					havingList.add (rHaving);
				}
				AndHaving andHaving = new AndHaving (havingList);
				having = andHaving;
			}
				break;
			case NodeTypes.OR_NODE: {
				OrNode orNode = (OrNode) vNode;
				ValueNode lNode = orNode.getLeftOperand();
				ValueNode rNode = orNode.getRightOperand();
				BaseHaving lHaving = valueNodeToHaving (lNode, aggregateNames, postAggregators, tableColumnsMeta);
				BaseHaving rHaving = valueNodeToHaving (rNode, aggregateNames, postAggregators, tableColumnsMeta);
				List<BaseHaving> havingList = Lists.newArrayList();
				if (lHaving instanceof OrHaving) {
					havingList.addAll (((OrHaving)lHaving).getHavingSpecs());
				} else {
					havingList.add (lHaving);
				}
				if (rHaving instanceof OrHaving) {
					havingList.addAll (((OrHaving)rHaving).getHavingSpecs());
				} else {
					havingList.add (rHaving);
				}
				OrHaving orHaving = new OrHaving (havingList);
				having = orHaving;
			}
				break;
			case NodeTypes.NOT_NODE: {
				NotNode notNode = (NotNode) vNode;
				ValueNode oNode = notNode.getOperand();
				BaseHaving oHaving = valueNodeToHaving (oNode, aggregateNames, postAggregators, tableColumnsMeta);
				having = new NotHaving (oHaving);
			}
				break;
			default:
				throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
						+"Un-Support Having.");
			}
		}
		return having;
	}
}
