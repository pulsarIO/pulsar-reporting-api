/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.sql;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.analytics.constants.Constants.AggregateFunction;
import com.ebay.pulsar.analytics.constants.ConstantsUtils;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.TableDimension;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.AggregateNode;
import com.foundationdb.sql.parser.AllResultColumn;
import com.foundationdb.sql.parser.BinaryArithmeticOperatorNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.FromList;
import com.foundationdb.sql.parser.FromTable;
import com.foundationdb.sql.parser.GroupByColumn;
import com.foundationdb.sql.parser.GroupByList;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.NumericConstantNode;
import com.foundationdb.sql.parser.OrderByList;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SelectNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.TableName;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.parser.Visitable;
import com.foundationdb.sql.parser.Visitor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 * @author mingmwang
 *
 */
public abstract class SQLTranslator {
	private static final Logger logger = LoggerFactory
			.getLogger(SQLTranslator.class);
	public static int DEFAULT_LIMIT = 10;
	public static final Map<String, String> arithFuncMap;
	static {
		arithFuncMap = Maps.newHashMap();
		arithFuncMap.put("+", "add");
		arithFuncMap.put("-", "sub");
		arithFuncMap.put("*", "mul");
		arithFuncMap.put("/", "div");
	}

	public class ResultInfo {
		private final Map<String, String> columnsMap;
		private final Map<String, String> dimensionsToAliasMap;
		private List<String> dimensions;
		private final Map<String, AggregateNode> aggregateNodesMap;
		private final Map<String, ValueNode> postAggregatesMap;
		private final Map<String, String> aggrKeyToAliasMap;
		private final String firstAlias;

		public ResultInfo(Map<String, String> columnsMap,
				Map<String, String> dimensionsToAliasMap,
				List<String> dimensions,
				Map<String, AggregateNode> aggregateNodesMap,
				Map<String, ValueNode> postAggregatesMap,
				Map<String, String> aggrKeyToAliasMap, String firstAlias) {
			this.columnsMap = columnsMap;
			this.dimensionsToAliasMap=dimensionsToAliasMap;
			this.dimensions = dimensions;
			this.aggregateNodesMap = aggregateNodesMap;
			this.postAggregatesMap = postAggregatesMap;
			this.aggrKeyToAliasMap = aggrKeyToAliasMap;
			this.firstAlias = firstAlias;
		}

		public Map<String, String> getColumnsMap() {
			return this.columnsMap;
		}
		public Map<String, String> getDimensionsToAliasMap() {
			return this.dimensionsToAliasMap;
		}

		public Map<String, AggregateNode> getSimpleAggregateColsMap() {
			Map<String, AggregateNode> result = Maps.newLinkedHashMap();
			for(Map.Entry<String, AggregateNode> entry : aggregateNodesMap.entrySet()){
				if(!postAggregatesMap.containsKey(entry.getKey())){
					result.put(aggrKeyToAliasMap.get(entry.getKey()), entry.getValue());
				}
			}
			return result;
		}

		public Map<String, AggregateNode> getAggregateNodesMap() {
			return this.aggregateNodesMap;
		}

		public Map<String, ValueNode> getPostAggregatesMap() {
			return this.postAggregatesMap;
		}

		public Map<String, String> getAggrKeyToAliasMap() {
			return this.aggrKeyToAliasMap;
		}

		public List<String> getDimensions() {
			return this.dimensions;
		}

		public String getFirstAlias() {
			return this.firstAlias;
		}
	}

	public class QueryDescription {

		/* Member variables: */
		private SelectNode selectNode;
		private CursorNode cursorNode;

		/* Constructors: */
		/**
		 * @param statement
		 * @param fromTable
		 * @param fields
		 */
		public QueryDescription(CursorNode cursorNode, SelectNode selectNode) {
			this.cursorNode = cursorNode;
			this.selectNode = selectNode;
		}

		/**
		 * @return the cursorNode
		 */
		public CursorNode getCursorNode() {
			return cursorNode;
		}

		/**
		 * @return the selectNode
		 */
		public SelectNode getSelectNode() {
			return selectNode;
		}
	}

	public class VisitorExtra implements Visitor {

		// Temporary stores the QueryDescription parameters
		SelectNode selectNode = null;
		CursorNode cursorNode = null;
		QueryDescription description = null;

		public VisitorExtra() {
		}

		@Override
		public Visitable visit(Visitable visitable)
				throws SqlTranslationException {
			QueryTreeNode node = (QueryTreeNode) visitable;

			if (visitable instanceof SelectNode) {
				SelectNode snode = (SelectNode) node;
				selectNode = snode;
			}
			int type = node.getNodeType();
			switch (type) {
			case NodeTypes.CURSOR_NODE: {

				cursorNode = (CursorNode) node;
			}
				break;
			default:
			}

			return node;
		}

		@Override
		public boolean stopTraversal() {
			return false;
		}

		@Override
		public boolean visitChildrenFirst(Visitable arg0) {
			return false;
		}

		@Override
		public boolean skipChildren(Visitable arg0) {
			return false;
		}

		public QueryDescription getQueryDescription()
				throws SqlTranslationException {
			this.description = new QueryDescription(cursorNode, selectNode);
			return this.description;
		}
	}

	public String getTableName(SelectNode selectNode) {
		FromList fromList = selectNode.getFromList();
		String fromTableName = null;

		if (fromList == null) {
			throw new SqlTranslationException(
					ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
							+ "Invalid table name.");
		} else {
			if (fromList.size() != 1) {
				throw new SqlTranslationException(
						ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
								+ "Invalid table name.");
			} else {
				FromTable fromTable = fromList.get(0);
				TableName tableName = fromTable.getOrigTableName();
				if (!tableName.hasSchema()) {
					fromTableName = tableName.getTableName();
				} else {
					fromTableName = tableName.getFullTableName();
				}
			}
		}
		return fromTableName;
	}

	public String getTableName(String sql) throws SqlTranslationException {
		QueryDescription queryDesc = parse(sql);
		SelectNode selectNode = queryDesc.getSelectNode();
		String tableName = getTableName(selectNode);
		return tableName;
	}

	public ResultInfo parseResultList(SelectNode selectNode,
			Table tableColumnsMeta) throws SqlTranslationException {
		return parseResultList(selectNode, tableColumnsMeta, null);
	}

	public ResultInfo parseResultList(SelectNode selectNode,
			Table tableColumnsMeta, Map<Integer, QueryTreeNode> nodesMapIn)
			throws SqlTranslationException {
		ValueNode vNode = null;
		// Aggregate Key "sum:count" to AggregateNode Map
		Map<String, AggregateNode> aggregateNodesMap = Maps.newLinkedHashMap();
		// Aggregate Key "sum:count" to Alias Name Map
		Map<String, String> aggrKeyToAliasMap = Maps.newLinkedHashMap();
		// Alias Name to AggregateNode Map
		Map<String, ValueNode> postAggregatesMap = Maps.newLinkedHashMap();

		Map<String, String> columnsMap = Maps.newHashMap();
		Map<String, String> dimensionsToAliasMap = Maps.newHashMap();
		List<String> dimensions = Lists.newArrayList();
		
		Map<String, BinaryArithmeticOperatorNode> compositeAggregateNodes = Maps.newLinkedHashMap();
		Set<String> aliasNames = Sets.newHashSet();

		String firstAlias = null;
		ResultColumnList resultList = selectNode.getResultColumns();
		for (ResultColumn resultCol : resultList) {
			String aliasName = resultCol.getName();
			if(aliasName != null){
				aliasName = aliasName.toLowerCase();
			}
			vNode = resultCol.getExpression();
			if (resultCol instanceof AllResultColumn) {
				// SELECT * FROM will get Exception
				throw new SqlTranslationException(
						ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
								+ "Specific columns required - select * (all) not supported.");
			} else if (vNode instanceof ColumnReference) {
				ColumnReference column = (ColumnReference) vNode;
				String colNameChanged = columnRefCheck(column,
						tableColumnsMeta, nodesMapIn, false, false);
				if(!aliasName.equals(column.getColumnName().toLowerCase())){
					if(aliasNames.contains(aliasName)){
						throw new SqlTranslationException(
								ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
										+ "Duplicated alias name:" + aliasName);
					}else{
						aliasNames.add(aliasName);
					}
				}
				dimensions.add(column.getColumnName());
				dimensionsToAliasMap.put(column.getColumnName(), aliasName);
				if(aliasName.equals(column.getColumnName().toLowerCase())){
					columnsMap.put(column.getColumnName().toLowerCase(), colNameChanged);
				}else{
					columnsMap.put(column.getColumnName().toLowerCase(), aliasName);
				}
			} else if (vNode instanceof BinaryArithmeticOperatorNode) {
				if (aliasName == null) {
					throw new SqlTranslationException(
							ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
									+ "Alias name required for Composite Aggregate Result.");
				}else if(aliasNames.contains(aliasName)){
					throw new SqlTranslationException(
							ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
									+ "Duplicated alias name:" + aliasName);
				}
				aliasNames.add(aliasName);
				compositeAggregateNodes.put(aliasName, (BinaryArithmeticOperatorNode)vNode);
				if (firstAlias == null) {
					firstAlias = aliasName;
				}
			} else if (vNode instanceof AggregateNode) {
				if (aliasName == null) {
					throw new SqlTranslationException(
							ExceptionErrorCode.SQL_PARSING_ERROR
									.getErrorMessage()
									+ "Alias name required for Aggregated Result.");
				}else if(aliasNames.contains(aliasName)){
					throw new SqlTranslationException(
							ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
									+ "Duplicated alias name:" + aliasName);
				}
				aliasNames.add(aliasName);
				String key = addSingleAggregateNode((AggregateNode) vNode,
						aggregateNodesMap, nodesMapIn, tableColumnsMeta);
				if (!aggrKeyToAliasMap.containsKey(key)) {
					aggrKeyToAliasMap.put(key, aliasName);
				}
				if (firstAlias == null) {
					firstAlias = aliasName;
				}
			} else {
				logger.info("Other type: " + vNode.getType());
			}
		}
		
		for(Map.Entry<String, BinaryArithmeticOperatorNode> entry : compositeAggregateNodes.entrySet()){
			String key = addCompositeAggregateNode(entry.getValue(),
					aggregateNodesMap, aggrKeyToAliasMap, nodesMapIn, tableColumnsMeta, true);
			if (!aggrKeyToAliasMap.containsKey(key)) {
				aggrKeyToAliasMap.put(key, entry.getKey());
			}
			postAggregatesMap.put(entry.getKey(), entry.getValue());
		}

		ResultInfo parseInfo = new ResultInfo(columnsMap, dimensionsToAliasMap, dimensions,
				aggregateNodesMap, postAggregatesMap, aggrKeyToAliasMap,
				firstAlias);

		return parseInfo;
	}

	public String addCompositeAggregateNode(ValueNode vNode,
			Map<String, AggregateNode> aggregateNodesMap,
			Map<String, String> aggrKeyToAliasMap,
			Map<Integer, QueryTreeNode> nodesMapIn, Table tableColumnsMeta,
			boolean toValidate) throws SqlTranslationException {
		String key = null;

		if (vNode instanceof BinaryArithmeticOperatorNode) {
			StringBuilder sb = new StringBuilder();
			BinaryArithmeticOperatorNode bNode = (BinaryArithmeticOperatorNode) vNode;
			ValueNode lNode = bNode.getLeftOperand();
			if (lNode instanceof ColumnReference) {
				ColumnReference colNode = (ColumnReference) lNode;
				if (toValidate && nodesMapIn != null) {
					columnRefCheck(colNode, tableColumnsMeta, nodesMapIn, false, false);
				}
				String colName = colNode.getColumnName().toLowerCase();
				sb.append(colName);
			} else {
				String keyL = addCompositeAggregateNode(lNode,
						aggregateNodesMap, aggrKeyToAliasMap, nodesMapIn, tableColumnsMeta,
						toValidate);
				sb.append(keyL);
			}
			// Add Operator
			sb.append("_");
			String operator = bNode.getOperator();
			sb.append(arithFuncMap.get(operator));
			sb.append("_");

			// Add Right Node
			ValueNode rNode = bNode.getRightOperand();
			if (rNode instanceof ColumnReference) {
				ColumnReference colNode = (ColumnReference) rNode;
				if (toValidate && nodesMapIn != null) {
					columnRefCheck(colNode, tableColumnsMeta, nodesMapIn, false, false);
				}
				String colName = colNode.getColumnName().toLowerCase();
				sb.append(colName);
			} else {
				String keyR = addCompositeAggregateNode(rNode,
						aggregateNodesMap, aggrKeyToAliasMap, nodesMapIn, tableColumnsMeta,
						toValidate);
				sb.append(keyR);
			}
			key = sb.toString();
			if(logger.isDebugEnabled()){
				logger.debug("Operator: " + operator);
			}
		} else if (vNode instanceof AggregateNode) {
			key = addSingleAggregateNode((AggregateNode) vNode,
					aggregateNodesMap, nodesMapIn, tableColumnsMeta);
			if(nodesMapIn == null){
				if(!aggrKeyToAliasMap.containsKey(key)){
					aggrKeyToAliasMap.put(key, "alias_" + (aggrKeyToAliasMap.size()+1));
				}
			}
		} else {
			logger.info("Other type: ");
		}
		return key;
	}

	public String addSingleAggregateNode(AggregateNode aNode,
			Map<String, AggregateNode> aggregateNodesMap,
			Map<Integer, QueryTreeNode> nodesMapIn, Table tableColumnsMeta)
			throws SqlTranslationException {
		String key = getAggregateKey(aNode, tableColumnsMeta, nodesMapIn,
				true);
		if (aggregateNodesMap != null && !aggregateNodesMap.containsKey(key)) {
			aggregateNodesMap.put(key, aNode);
		}
		return key;
	}

	public String getAggregateKey(AggregateNode aNode, Table tableColumnsMeta,
			Map<Integer, QueryTreeNode> nodesMapIn, boolean validate) throws SqlTranslationException {
		String funcName = aNode.getAggregateName().toLowerCase();

		AggregateFunction aggregateFunc = ConstantsUtils
				.getAggregateFunction(funcName);
		if (validate && aggregateFunc == null) {
			throw new SqlTranslationException(
					ExceptionErrorCode.INVALID_AGGREGATE.getErrorMessage()
							+ "Non-supported aggregate function.");
		}

		if (aggregateFunc == AggregateFunction.countall) {
			return AggregateFunction.countall.name();
		}
		StringBuilder sb = new StringBuilder(funcName);

		ValueNode colNode = aNode.getOperand();
		if (colNode instanceof ColumnReference) {
			ColumnReference column = (ColumnReference) colNode;
			if (validate) {
				columnRefCheck(column, tableColumnsMeta, nodesMapIn, !aNode.isDistinct(), tableColumnsMeta.isColumnHyperLogLog(column.getColumnName()));
			}
			String colName = column.getColumnName().toLowerCase();
			sb.append(':');
			if (aNode.isDistinct()) {
				sb.append("distinct_");
			}
			sb.append(colName);
			if(logger.isDebugEnabled()){
				logger.debug("Aggregate: " + funcName);
			}
		} else if (colNode instanceof NumericConstantNode) {
			NumericConstantNode cNode = (NumericConstantNode) colNode;
			Object constObj = cNode.getValue();
			if (constObj instanceof Integer) {
				int value = (Integer) constObj;
				if (value != 1) {
					throw new SqlTranslationException(
							ExceptionErrorCode.SQL_PARSING_ERROR
									.getErrorMessage()
									+ "Only count(1) supported.");
				}
				sb.append(":1");
			} else {
				throw new SqlTranslationException(
						ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
								+ "count (1) : Integer value required.");
			}
		} else {
			throw new SqlTranslationException(
					ExceptionErrorCode.INVALID_AGGREGATE.getErrorMessage()
							+ "Aggregate Column Ref required.");
		}
		return sb.toString();
	}

	public int getIntValue(ValueNode valueNode, int defaultValue)
			throws SqlTranslationException {
		int value = -1;
		if (valueNode != null && valueNode instanceof NumericConstantNode) {
			NumericConstantNode cNode = (NumericConstantNode) valueNode;
			Object constObj = cNode.getValue();
			if (constObj instanceof Integer) {
				value = (Integer) constObj;
				if (value <= 0) {
					throw new SqlTranslationException(
							"Integer value > 0 required.");
				}
			} else {
				throw new SqlTranslationException("Integer value required.");
			}
		} else {
			value = defaultValue;
		}
		return value;
	}
	
	public String columnRefCheck(ColumnReference column,
			Table tableColumnsMeta, Map<Integer, QueryTreeNode> nodesMapIn,
			ConstantNode cNode) throws SqlTranslationException {
		TableDimension tableDimension = tableColumnsMeta.getDimensionByName(column
				.getColumnName());
		if (tableDimension == null) {
			throw new SqlTranslationException(
					ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
							+ column.getColumnName());
		}
		if(cNode != null && cNode.getValue() == null){
			throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
					+"Un-Support Filter");
		}
		String colName = checkNameChange(column, tableColumnsMeta, nodesMapIn);
		if(cNode != null){
			if (tableColumnsMeta.isColumnNumeric(colName)) {
				if (!(cNode instanceof NumericConstantNode)) {
					throw new SqlTranslationException(
							ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
									+ "Error String Value for Numeric Dimension: "
									+ column.getColumnName());
				}
			} else {
				if (cNode instanceof NumericConstantNode) {
					throw new SqlTranslationException(
							ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
									+ "Error Numeric Value for String Dimension : "
									+ column.getColumnName());
				}
			}
		}
		return colName;
	}

	public String columnRefCheck(ColumnReference column,
			Table tableColumnsMeta, Map<Integer, QueryTreeNode> nodesMapIn,
			boolean aggrColumn, boolean hll) throws SqlTranslationException {
		TableDimension tableDimension = null;
		ExceptionErrorCode errorCode = null;
		if(aggrColumn || hll){
			tableDimension = tableColumnsMeta.getMetricByName(column.getColumnName());
			errorCode = ExceptionErrorCode.INVALID_METRIC;
		}
		else{
			tableDimension = tableColumnsMeta.getDimensionByName(column.getColumnName());
			errorCode = ExceptionErrorCode.INVALID_DIMENSION;
		}
		if (tableDimension == null) {
			// Not valid field name (dimension/metric)
			throw new SqlTranslationException(errorCode.getErrorMessage() + column.getColumnName());
		} else {
			return checkNameChange(column, tableColumnsMeta, nodesMapIn);
		}
	}
	
	public void groupByCheck(SelectNode selectNode, CursorNode cursorNode, Map<String, String> columnsMap) {
		OrderByList orderByList = cursorNode.getOrderByList();
		GroupByList groupByList = selectNode.getGroupByList();
		if (groupByList == null || groupByList.size() == 0) {
			if((orderByList !=null && orderByList.size() > 0)) {
				throw new SqlTranslationException(
						ExceptionErrorCode.SQL_PARSING_ERROR
								.getErrorMessage()
								+ "'Order By can only be used with Group By.' ");
			}
			if (columnsMap.size() > 0) {
				throw new SqlTranslationException(
						ExceptionErrorCode.INVALID_AGGREGATE.getErrorMessage()
								+ "Group By columns required for dimensions: "
								+ columnsMap.values());
			}
		} else {
			Set<String> groupByColSet = Sets.newHashSet();
			int listSize = groupByList.size();
			for (int i = 0; i < listSize; i++) {
				GroupByColumn groupByCol = (GroupByColumn) groupByList.get(i);
				ValueNode vNode = groupByCol.getColumnExpression();
				if (vNode instanceof ColumnReference) {
					ColumnReference column = (ColumnReference) vNode;
					String colName = column.getColumnName();
					if (!columnsMap.containsKey(colName)) {
						throw new SqlTranslationException(
								ExceptionErrorCode.INVALID_AGGREGATE
										.getErrorMessage()
										+ "Group By column not found in Selection: "
										+ colName);
					}
					groupByColSet.add(colName);
				} else {
					throw new SqlTranslationException(
							ExceptionErrorCode.INVALID_AGGREGATE.getErrorMessage()
									+ "No column refernce found for Group By.");
				}
			}
			for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
				String key = entry.getKey();
				if (!groupByColSet.contains(key)) {
					throw new SqlTranslationException(
							ExceptionErrorCode.INVALID_AGGREGATE
									.getErrorMessage()
									+ "Group By column required for dimension in Selection: "
									+ key);
				}
			}
		}
	}

	public String checkNameChange(ColumnReference column,
			Table tableColumnsMeta, Map<Integer, QueryTreeNode> nodesMapIn){
		return tableColumnsMeta.getColumnMeta(column.getColumnName()).getName();
	}

	public QueryDescription parse(String sql) throws SqlTranslationException {
		SQLParser parser = new SQLParser();
		StatementNode stmt;
		try {
			stmt = parser.parseStatement(sql);
			VisitorExtra vx = new VisitorExtra();
			Visitor v = (Visitor) vx;
			stmt.accept(v);
			QueryDescription description = vx.getQueryDescription();
			return description;
		} catch (StandardException e) {
			throw new SqlTranslationException(ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage() + e.getMessage());
		}
	}
}
