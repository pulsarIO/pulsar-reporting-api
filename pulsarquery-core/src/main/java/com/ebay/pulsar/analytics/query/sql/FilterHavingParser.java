/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.sql;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.BinaryRelationalOperatorNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.LikeEscapeOperatorNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.NotNode;
import com.foundationdb.sql.parser.NumericConstantNode;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SelectNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.parser.Visitable;
import com.foundationdb.sql.parser.Visitor;
import com.google.common.collect.Lists;

/**
 * Parse the filter and having part for sql query.
 *
 */
public class FilterHavingParser {
	public class QueryWhereHaving {

		/* Member variables: */
		private List<ValueNode> whereClauses;
		private List<ValueNode> havingClauses;
		private Map<Integer, QueryTreeNode> nodeMaps;

		/* Constructors: */
		/**
		 * @param whereClauses
		 * @param havingClauses
		 * @param nodeMaps
		 */
		public QueryWhereHaving(List<ValueNode> whereClauses, List<ValueNode> havingClauses, Map<Integer, QueryTreeNode> nodeMaps) {
			this.whereClauses = whereClauses;
			this.havingClauses = havingClauses;
			this.nodeMaps = nodeMaps;
		}

		/**
		 * @return the whereClauses
		 */
		public List<ValueNode> getWhereClauses() {
			return whereClauses;
		}

		/**
		 * @return the havingClauses
		 */
		public List<ValueNode> getHavingClauses() {
			return havingClauses;
		}

		/**
		 * @return the nodeMaps
		 */
		public Map<Integer, QueryTreeNode> getNodeMaps() {
			return nodeMaps;
		}

	}

	public class VisitorExtra implements Visitor {
		List<ValueNode> whereClauses = Lists.newArrayList();
		List<ValueNode> havingClauses =Lists.newArrayList();
		List<QueryTreeNode> likeNodes = Lists.newArrayList();
		List<QueryTreeNode> notNodes =Lists.newArrayList();
		List<QueryTreeNode> binaryRelationNodes = Lists.newArrayList();
		Map<Integer, QueryTreeNode> nodeMaps = new TreeMap<Integer, QueryTreeNode> (new Comparator<Integer>()
				{
					// Reverse the Key -- Offset starting from rightmost first
					public int compare (Integer a, Integer b) {
						return b.compareTo(a);
					}
				});

		Table tableMeta = null;
		QueryWhereHaving description = null;
		
		VisitorExtra (Table tableMeta) {
			this.tableMeta = tableMeta;
		}

		@Override
		public Visitable visit(Visitable visitable) throws SqlTranslationException {
			QueryTreeNode node = (QueryTreeNode) visitable;

			if (visitable instanceof SelectNode) {
				SelectNode snode = (SelectNode) node;
				ValueNode vNode = snode.getWhereClause();
				if (vNode != null) {
					whereClauses.add(vNode);
				} else {
					vNode = snode.getHavingClause();
					if (vNode != null) {
						havingClauses.add(vNode);
					}
				}
			}
			if (visitable instanceof BinaryRelationalOperatorNode) {
				binaryRelationNodes.add(node);
			}
			int type = node.getNodeType();
			switch (type) {
			case NodeTypes.FROM_BASE_TABLE: {
			}
			break;
			case NodeTypes.COLUMN_REFERENCE: {
				ColumnReference column = (ColumnReference) node;
				String name = column.getColumnName().toLowerCase();
				if (tableMeta != null && tableMeta.getDimensionByName(name) != null) {
					Integer offset = node.getBeginOffset();
					nodeMaps.put(offset, node);
				}else{
					if(isColumnCheck()){
						throw new SqlTranslationException (ExceptionErrorCode.INVALID_FILTER.getErrorMessage()
								+"Invalid Dimension Name in Filter: " + name);
					}
				}
			}
			break;
			case NodeTypes.LIKE_OPERATOR_NODE: {
				likeNodes.add(node);
			}
			break;
			case NodeTypes.NOT_NODE: {
				notNodes.add(node);
			}
			break;
			case NodeTypes.CURSOR_NODE: {
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

		public QueryWhereHaving getQueryWhereHaving (Table tableMeta, Set<String> aggregateSet)
				throws SqlTranslationException {
				ExceptionErrorCode errorCode = ExceptionErrorCode.INVALID_FILTER;
				if(aggregateSet != null){
					errorCode = ExceptionErrorCode.INVALID_HAVING_CLAUSE;
				}
			if (this.binaryRelationNodes.size() > 0) {
				for (QueryTreeNode node: binaryRelationNodes) {
					BinaryRelationalOperatorNode bNode = (BinaryRelationalOperatorNode)node;
					ValueNode lNode = bNode.getLeftOperand();
					ValueNode rNode = bNode.getRightOperand();
					ConstantNode cNode = null;
					ColumnReference column = null;

					if (lNode instanceof ColumnReference) {
						column = (ColumnReference) lNode;
						if (rNode instanceof ConstantNode) {
							cNode = (ConstantNode) rNode;
						}
					} else if (rNode instanceof ColumnReference) {
						column = (ColumnReference) rNode;
						if (lNode instanceof ConstantNode) {
							cNode = (ConstantNode) lNode;
						}
					} else {
						// No Column Error
						throw new SqlTranslationException (errorCode.getErrorMessage()
								+"No Dimension Reference.");
					}
					String colName = column.getColumnName();
					if (aggregateSet != null) {
						if (aggregateSet.contains(colName)) {
							nodeMaps.put(column.getBeginOffset(), column);
							continue;
						} else {
							throw new SqlTranslationException (ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage()
									+"Invalid Aggregate in Having: " + colName);
						}
					}
					if (!columnCheck) {
						continue;
					}
					if (tableMeta.getDimensionByName(colName) == null &&  tableMeta.getMetricByName(colName) == null) {
						// Not valid dimension
						throw new SqlTranslationException (errorCode.getErrorMessage()
								+"Invalid Dimension Name: " + colName);
					} else {
						if (cNode == null || cNode.getValue() == null) {
							throw new SqlTranslationException (errorCode.getErrorMessage()
									+ "Un-Support constant value: null");
						} else {
							if (tableMeta.isColumnNumeric(colName)) {
								if (!(cNode instanceof NumericConstantNode)) {
									throw new SqlTranslationException (errorCode.getErrorMessage()
											+"Error String Value for Numeric Dimension: " + colName);
								}
							} else {
								if (cNode instanceof NumericConstantNode) {
									throw new SqlTranslationException (errorCode.getErrorMessage()
											+"Error Numeric Value for String Dimension : " + colName);
								}
							}
						}
					}
				}
			}
			if (tableMeta != null) {
				for (QueryTreeNode node: likeNodes) {
					LikeEscapeOperatorNode lNode = (LikeEscapeOperatorNode) node;
					ValueNode vNode = lNode.getReceiver();
					ColumnReference column = (ColumnReference) vNode;
					Integer offset = column.getBeginOffset();
					nodeMaps.put(offset, node);
				}
				for (QueryTreeNode node: notNodes) {
					NotNode nNode = (NotNode) node;
					nodeMaps.put(nNode.getBeginOffset(), node);
					nodeMaps.put(nNode.getEndOffset(), node);
				}
			}
			this.description = new QueryWhereHaving(whereClauses, havingClauses, nodeMaps);
			return this.description;
		}
	}
	
	protected QueryWhereHaving parse (String sql, Table tableMeta) throws SqlTranslationException {
		return parse(sql, tableMeta, null);
	}

	protected QueryWhereHaving parse (String sql, Table tableMeta, Set<String> aggregateSet) throws SqlTranslationException {
		SQLParser parser = new SQLParser();
		try {
			StatementNode stmt = parser.parseStatement(sql);
			VisitorExtra vx = new VisitorExtra(tableMeta);
			Visitor v = (Visitor) vx;
			stmt.accept(v);
			QueryWhereHaving description = vx.getQueryWhereHaving(tableMeta, aggregateSet);
			return description;
		} catch (StandardException e) {
			throw new SqlTranslationException(e.getMessage(), e);
		}
	}
	
	
	public boolean isColumnCheck() {
		return columnCheck;
	}

	public void setColumnCheck(boolean columnCheck) {
		this.columnCheck = columnCheck;
	}

	private boolean columnCheck = true;
	
	public static final String SELECT_FROM_WHERE = "SELECT * FROM TBL WHERE ";
	public static final String SELECT_FROM_GROUPBY_HAVING = "SELECT * FROM TBL GROUP BY ABC HAVING ";
}
