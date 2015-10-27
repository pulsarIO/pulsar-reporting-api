/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import java.util.List;
import java.util.Properties;

import com.ebay.pulsar.analytics.query.client.ClientQueryConfig;

/**
 * 
 * @author mingmwang
 *
 */
public class DataSourceConfiguration {
	private DataSourceTypeEnum dataSourceType;
	private String dataSourceName;
	private List<String> endPoint;
	
	private Properties properties;
	private boolean realOnly = false; 
	private long refreshTime;
	
	private ClientQueryConfig clientConfigure = new ClientQueryConfig();

	public DataSourceConfiguration(DataSourceTypeEnum dataSourceType, String dataSourceName) {
		this.dataSourceType = dataSourceType;
		this.dataSourceName = dataSourceName;
	}
	
	public DataSourceTypeEnum getDataSourceType() {
		return dataSourceType;
	}
	
	public void setDataSourceType(DataSourceTypeEnum dataSourceType) {
		this.dataSourceType = dataSourceType;
	}

	public String getDataSourceName() {
		return dataSourceName;
	}

	public void setDataSourceName(String dataSourceName) {
		this.dataSourceName = dataSourceName;
	}

	public List<String> getEndPoint() {
		return endPoint;
	}
	
	public void setEndPoint(List<String> endPoint) {
		this.endPoint = endPoint;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	public boolean isRealOnly() {
		return realOnly;
	}

	public void setRealOnly(boolean realOnly) {
		this.realOnly = realOnly;
	}

	public long getRefreshTime() {
		return refreshTime;
	}

	public void setRefreshTime(long refreshTime) {
		this.refreshTime = refreshTime;
	}
	
	public ClientQueryConfig getClientConfigure() {
		return clientConfigure;
	}

	public void setClientConfigure(ClientQueryConfig clientConfigure) {
		this.clientConfigure = clientConfigure;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dataSourceName == null) ? 0 : dataSourceName.hashCode());
		result = prime * result
				+ ((dataSourceType == null) ? 0 : dataSourceType.hashCode());
		result = prime * result
				+ ((endPoint == null) ? 0 : endPoint.hashCode());
		result = prime * result
				+ ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + (realOnly ? 1231 : 1237);
		result = prime * result + (int) (refreshTime ^ (refreshTime >>> 32));
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
		DataSourceConfiguration other = (DataSourceConfiguration) obj;
		if (dataSourceName == null) {
			if (other.dataSourceName != null)
				return false;
		} else if (!dataSourceName.equals(other.dataSourceName))
			return false;
		if (dataSourceType == null) {
			if (other.dataSourceType != null)
				return false;
		} else if (!dataSourceType.equals(other.dataSourceType))
			return false;
		if (endPoint == null) {
			if (other.endPoint != null)
				return false;
		} else if (!endPoint.equals(other.endPoint))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		if (realOnly != other.realOnly)
			return false;
		if (refreshTime != other.refreshTime)
			return false;
		return true;
	}
}
