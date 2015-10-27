/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 * @author mingmwang
 *
 */
public class Table {
	public static final int BIGINT = java.sql.Types.BIGINT;
	public static final int DOUBLE = java.sql.Types.DOUBLE;
	public static final int VARCHAR = java.sql.Types.VARCHAR;
	
	private static final String SUFFIX_HLL = "_hll";

	private String tableName;
	private String dateColumn;
	private boolean noInnerJoin = false;
	
	protected Set<String> tableNames = Sets.newHashSet();
	private Collection<? extends TableDimension> dimensions;
	private Map<String, TableDimension> dimMetaMap = Maps.newHashMap();
	 
	private Collection<? extends TableDimension> metrics;
	private Map<String, TableDimension> metricMetaMap = Maps.newHashMap();;
	
	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
		tableNames.add(tableName);
	}
	
	public String getDateColumn() {
		return dateColumn;
	}

	public void setDateColumn(String dateColumn) {
		this.dateColumn = dateColumn;
	}

	public boolean isNoInnerJoin() {
		return noInnerJoin;
	}

	public void setNoInnerJoin(boolean noInnerJoin) {
		this.noInnerJoin = noInnerJoin;
	}
	
	public Set<String> getAllTableNames() {
		return tableNames;
	}
	

	public Collection<? extends TableDimension> getDimensions() {
		return dimensions;
	}
	
	public void setDimensions(Collection<? extends TableDimension> dimensions) {
		for(TableDimension dim : dimensions){
			if(!insertDimensionMap(dim)){
				throw new IllegalArgumentException("TableDimension name conflicts!");
			}
		}
		this.dimensions = dimensions;
	}
		
	public boolean insertDimensionMap(TableDimension dimension){
		for(String name : dimension.getColumnNames()){
			if(dimMetaMap.get(name) != null){
				return false;
			}else{
				dimMetaMap.put(name, dimension);
			}
		}
		return true;
	}
	
	public Collection<? extends TableDimension> getMetrics() {
		return metrics;
	}
	
	public void setMetrics(Collection<? extends TableDimension> metrics) {
		for(TableDimension metric : metrics){
			if(!insertMetricMap(metric)){
				throw new IllegalArgumentException("TableMetric name conflicts!");
			}
		}
		this.metrics = metrics;
	}
	
	public boolean insertMetricMap(TableDimension metric) {
		for (String name : metric.getColumnNames()) {
			if (metricMetaMap.get(name) != null) {
				return false;
			}else{
				metricMetaMap.put(name, metric);
			}
		}
		return true;
	}
	
	public TableDimension getDimensionByName(String dimensionName){
		return dimMetaMap.get(dimensionName);
	}

	public TableDimension getMetricByName(String metricName){
		return metricMetaMap.get(metricName);
	}
	
	public TableDimension getColumnMeta(String metricName){
		TableDimension tableDimension = dimMetaMap.get(metricName);
		TableDimension tableMetric = metricMetaMap.get(metricName);
		if(tableDimension != null)
			return tableDimension;
		else
			return tableMetric;		
	}

	public boolean isColumnMetric (String columnName) {
		return metricMetaMap.containsKey(columnName);
	}
	
	public Integer getColumnType (String columnName) {
		TableDimension column =  getColumnMeta(columnName);
		if(column == null)
			return null;
		else 
			return column.getType();
	}
	
	public boolean isColumnNumeric (String columnName) {
		return (isColumnInteger(columnName) || isColumnDouble(columnName));
	}

	public boolean isColumnInteger (String columnName) {
		if (columnName != null) {
			Integer columnType = getColumnType(columnName);
			if (columnType != null) {
				return (BIGINT == columnType);
			}
		}
		return false;
	}

	public boolean isColumnDouble (String columnName) {
		if (columnName != null) {
			Integer columnType = getColumnType(columnName);
			if (columnType != null) {
				return (DOUBLE == columnType);
			}
		}
		return false;
	}

	public boolean isColumnHyperLogLog (String columnName) {
		if (columnName != null) {
			return columnName.endsWith(SUFFIX_HLL);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dateColumn == null) ? 0 : dateColumn.hashCode());
		result = prime * result
				+ ((dimensions == null) ? 0 : dimensions.hashCode());
		result = prime * result + ((metrics == null) ? 0 : metrics.hashCode());
		result = prime * result + (noInnerJoin ? 1231 : 1237);
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result
				+ ((tableNames == null) ? 0 : tableNames.hashCode());
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
		Table other = (Table) obj;
		if (dateColumn == null) {
			if (other.dateColumn != null)
				return false;
		} else if (!dateColumn.equals(other.dateColumn))
			return false;
		if (dimensions == null) {
			if (other.dimensions != null)
				return false;
		} else if (!dimensions.equals(other.dimensions))
			return false;
		if (metrics == null) {
			if (other.metrics != null)
				return false;
		} else if (!metrics.equals(other.metrics))
			return false;
		if (noInnerJoin != other.noInnerJoin)
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (tableNames == null) {
			if (other.tableNames != null)
				return false;
		} else if (!tableNames.equals(other.tableNames))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Table [tableName=" + tableName + ", tableNames=" + tableNames
				+ ", dimensions=" + dimensions + ", metrics=" + metrics + "]";
	}
}
