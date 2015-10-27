/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.sql;

import java.util.List;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.metricstore.druid.filter.AndFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.NotFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.OrFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.RegexFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.SelectorFilter;
import com.foundationdb.sql.parser.AndNode;
import com.foundationdb.sql.parser.BinaryRelationalOperatorNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.InListOperatorNode;
import com.foundationdb.sql.parser.IsNullNode;
import com.foundationdb.sql.parser.LikeEscapeOperatorNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.NotNode;
import com.foundationdb.sql.parser.OrNode;
import com.foundationdb.sql.parser.RowConstructorNode;
import com.foundationdb.sql.parser.ValueNode;
import com.google.common.collect.Lists;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidFilterTranslator extends AbstractDruidSqlTranslator{

	public BaseFilter valueNodeToFilter (ValueNode vNode, Table tableColumnsMeta) throws SqlTranslationException {
		BaseFilter filter = null;
		int nodeType = vNode.getNodeType();
		if (vNode instanceof BinaryRelationalOperatorNode) {
			BinaryRelationalOperatorNode bNode = (BinaryRelationalOperatorNode) vNode;
			ValueNode lNode = bNode.getLeftOperand();
			ValueNode rNode = bNode.getRightOperand();
			String columnName = null;
			String value = null;
			if (lNode instanceof ColumnReference) {
				ColumnReference column = (ColumnReference) lNode;
				if (rNode instanceof ConstantNode) {
					ConstantNode cNode = (ConstantNode) rNode;
					columnName = columnRefCheck(column, tableColumnsMeta, null, cNode);
					value = cNode.getValue().toString();
				} else {
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
							+"Un-Support Filter");
				}
			} else if (rNode instanceof ColumnReference) {
				ColumnReference column = (ColumnReference) rNode;
				if (lNode instanceof ConstantNode ) {
					ConstantNode cNode = (ConstantNode) lNode;
					columnName = columnRefCheck(column, tableColumnsMeta, null, cNode);
					value = cNode.getValue().toString();
				} else {
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
							+"Un-Support Filter");
				}
			} else {
				throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
						+"Un-Support Filter");
			}

			switch (nodeType) {
			case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
				filter = new SelectorFilter (columnName, value);
				break;
			case NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:
				BaseFilter filterEq = new SelectorFilter (columnName, value);
				filter = new NotFilter (filterEq);
				break;
			default:
				throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
						+"Un-Support Binary Filter");
			}

		} else if (vNode instanceof LikeEscapeOperatorNode) {
			LikeEscapeOperatorNode likeNode = (LikeEscapeOperatorNode) vNode;
			ValueNode lNode = likeNode.getReceiver();
			ValueNode rNode = likeNode.getLeftOperand();
			String columnName = null;
			String value = null;

			if (lNode instanceof ColumnReference) {
				ColumnReference column = (ColumnReference) lNode;
				columnName = columnRefCheck(column, tableColumnsMeta, null, null);
				if (rNode instanceof ConstantNode) {
					ConstantNode cNode = (ConstantNode) rNode;
					value = cNode.getValue().toString();
				} else {
					throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
							+"Un-Support Regex Filter Constant");
				}
			} else {
				throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
						+"Un-Support Regex Filter Syntax");
			}
			filter = new RegexFilter (columnName, value);
		} else if (vNode instanceof IsNullNode) {
			IsNullNode iNode = (IsNullNode) vNode;
			ColumnReference column = (ColumnReference) iNode.getOperand();
			String columnName = columnRefCheck(column,tableColumnsMeta, null, null);
			filter = new SelectorFilter (columnName, null);
		} else if (vNode instanceof InListOperatorNode) {
			InListOperatorNode inNode = (InListOperatorNode) vNode;
			RowConstructorNode lNode = inNode.getLeftOperand();
			String columnName = null;
			String value = null;
			if (lNode.getNodeList().get(0) instanceof ColumnReference) {
				ColumnReference column = (ColumnReference) lNode.getNodeList().get(0);
				columnName = columnRefCheck(column, tableColumnsMeta, null, null);
				RowConstructorNode rNode = inNode.getRightOperandList();
				List<BaseFilter> filterList = Lists.newArrayList();
				for(ValueNode node:rNode.getNodeList()){
					if (node instanceof ConstantNode) {
						ConstantNode cNode = (ConstantNode) node;
						columnName = columnRefCheck(column, tableColumnsMeta, null, cNode);
						value = cNode.getValue().toString();
						BaseFilter inFilter=new SelectorFilter(columnName,value);
						filterList.add(inFilter);
					} else {
						throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
								+"Un-Support INList Filter Constant");
					}
				}
				filter = new OrFilter (filterList);

			} else {
				throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
						+"Un-Support INList Filter Syntax");
			}
		} else {
			switch (nodeType) {
			case NodeTypes.AND_NODE: {
				AndNode andNode = (AndNode) vNode;
				ValueNode lNode = andNode.getLeftOperand();
				ValueNode rNode = andNode.getRightOperand();
				BaseFilter lFilter = valueNodeToFilter (lNode, tableColumnsMeta);
				BaseFilter rFilter = valueNodeToFilter (rNode, tableColumnsMeta);
				List<BaseFilter> filterList = Lists.newArrayList();
				if (lFilter instanceof AndFilter) {
					filterList.addAll (((AndFilter)lFilter).getFields());
				} else {
					filterList.add (lFilter);
				}
				if (rFilter instanceof AndFilter) {
					filterList.addAll (((AndFilter)rFilter).getFields());
				} else {
					filterList.add (rFilter);
				}
				AndFilter andFilter = new AndFilter (filterList);
				filter = andFilter;
			}
				break;
			case NodeTypes.OR_NODE: {
				OrNode orNode = (OrNode) vNode;
				ValueNode lNode = orNode.getLeftOperand();
				ValueNode rNode = orNode.getRightOperand();
				BaseFilter lFilter = valueNodeToFilter (lNode, tableColumnsMeta);
				BaseFilter rFilter = valueNodeToFilter (rNode, tableColumnsMeta);
				List<BaseFilter> filterList = Lists.newArrayList();
				if (lFilter instanceof OrFilter) {
					filterList.addAll (((OrFilter)lFilter).getFields());
				} else {
					filterList.add (lFilter);
				}
				if (rFilter instanceof OrFilter) {
					filterList.addAll (((OrFilter)rFilter).getFields());
				} else {
					filterList.add (rFilter);
				}
				OrFilter orFilter = new OrFilter (filterList);
				filter = orFilter;
			}
				break;
			case NodeTypes.NOT_NODE: {
				NotNode notNode = (NotNode) vNode;
				ValueNode oNode = notNode.getOperand();
				BaseFilter oFilter = valueNodeToFilter (oNode, tableColumnsMeta);
				filter = new NotFilter (oFilter);
			}
				break;
			default:
				throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
						+"Un-Support Filter");
			}
		}
		return filter;
	}
	
	
}
