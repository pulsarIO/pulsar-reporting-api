/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource.loader;

import java.util.Properties;

import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.DataSourceProvider;
import com.ebay.pulsar.analytics.datasource.DataSourceTypeEnum;
import com.ebay.pulsar.analytics.exception.DataSourceConfigurationException;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * 
 * @author mingmwang
 *
 */
public class DynamicDataSourceConfigurationManager {
	public static void activateDataSource(DBDataSource datasource) {
		String databaseName = datasource.getName();
		String endPoints = datasource.getEndpoint();
		DataSourceTypeEnum  dataSourceTypeEnum = DataSourceTypeEnum.fromType(datasource.getType());
		if(dataSourceTypeEnum == null){
			throw new DataSourceConfigurationException("Unsupported dataSourceType:"+ datasource.getType());
		}
		DataSourceConfiguration configuration = new DataSourceConfiguration(dataSourceTypeEnum, databaseName);
		Properties clientProperties = datasource.getClientProperties(Properties.class);
		if(clientProperties != null){
			configuration.setProperties(clientProperties);
		}
		configuration.setEndPoint(Lists.newArrayList(Splitter.on(',')
				.trimResults().split(endPoints)));
		long refreshTime = getDBRefreshTime(datasource);
		configuration.setRefreshTime(refreshTime);
		
		DataSourceMetaRepo.getInstance().addDbConf(databaseName, configuration);
		DataSourceMetaRepo.getInstance().getDBMetaFromCache(databaseName);

	}

	public static void disableDataSource(DBDataSource datasource) {
		DataSourceProvider dataBase = DataSourceMetaRepo.getInstance().getDBMetaFromCache(
				datasource.getName());
		if (dataBase != null) {
			dataBase.close();
		}
		DataSourceMetaRepo.getInstance().disableDBMetaFromCache(datasource.getName());
	}
	
	public static long getDBRefreshTime(DBDataSource dataSource) {
		long refreshTime = 0;
		if(dataSource.getLastUpdateTime() != null){
			refreshTime = dataSource.getLastUpdateTime().getTime();
		}else{
			refreshTime = dataSource.getCreateTime().getTime();
		}
		return refreshTime;
	}
}
