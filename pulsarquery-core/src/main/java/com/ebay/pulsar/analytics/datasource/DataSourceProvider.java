/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * 
 * @author mingmwang
 *
 */
public class DataSourceProvider {
	private String dataSourceName;
	private Collection<Table> tables;
	private Map<String, Table> metaMap;
	private DBConnector connector;
	
	public DBConnector getConnector() {
		return connector;
	}

	public void setConnector(DBConnector connector) {
		this.connector = connector;
	}
	
	public String getDataSourceName() {
		return dataSourceName;
	}

	public void setDataSourceName(String dataSourceName) {
		this.dataSourceName = dataSourceName;
	}

	public Collection<Table> getTables() {
		return tables;
	}
	
	public Table getTableByName(String name){
		return metaMap.get(name);
	}
	
	public void close(){
		connector.close();
		tables.clear();
		metaMap.clear();
	}
	
	public void setTables(Collection<Table> tables) {
		this.tables = tables;
		metaMap = Maps.newHashMap();
		for(Table tab : tables){
			for (String name : tab.getAllTableNames()) {
				metaMap.put(name, tab);
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dataSourceName == null) ? 0 : dataSourceName.hashCode());
		result = prime * result + ((metaMap == null) ? 0 : metaMap.hashCode());
		result = prime * result + ((tables == null) ? 0 : tables.hashCode());
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
		DataSourceProvider other = (DataSourceProvider) obj;
		if (dataSourceName == null) {
			if (other.dataSourceName != null)
				return false;
		} else if (!dataSourceName.equals(other.dataSourceName))
			return false;
		if (metaMap == null) {
			if (other.metaMap != null)
				return false;
		} else if (!metaMap.equals(other.metaMap))
			return false;
		if (tables == null) {
			if (other.tables != null)
				return false;
		} else if (!tables.equals(other.tables))
			return false;
		return true;
	}
}
