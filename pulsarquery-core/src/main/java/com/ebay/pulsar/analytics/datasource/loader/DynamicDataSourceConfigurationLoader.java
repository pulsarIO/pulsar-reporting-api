/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource.loader;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.service.DBDataSourceService;
import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;

/**
 * Load all the DB configurations from datasources
 *
 * @author mingmwang
 * 
 */
public class DynamicDataSourceConfigurationLoader implements DataSourceConfigurationLoader {
	private static final Logger logger = LoggerFactory.getLogger(DynamicDataSourceConfigurationLoader.class);
	private DBDataSourceService datasourceService = new DBDataSourceService();

	@Override
	public void load() {
		List<DBDataSource> list = datasourceService.getAll();
		for(DBDataSource dataSource : list){
			String dataSourceName = dataSource.getName();			
			
			DataSourceConfiguration configFromRepo = DataSourceMetaRepo.getInstance().getDbConf(dataSourceName);
			if(configFromRepo == null){
				logger.info("Add dynamic metricstore:" + dataSourceName);
				DynamicDataSourceConfigurationManager.activateDataSource(dataSource);
			}else{
				if(configFromRepo.isRealOnly()){
					logger.error("Unable to update readonly metricstore:" + dataSourceName);
					continue;
				}else{
					long refreshTime = DynamicDataSourceConfigurationManager.getDBRefreshTime(dataSource);
					if(configFromRepo.getRefreshTime() < refreshTime){
						logger.info("Refresh dynamic metricstore:" + dataSourceName);
						DynamicDataSourceConfigurationManager.disableDataSource(dataSource);
						DynamicDataSourceConfigurationManager.activateDataSource(dataSource);
					}
				}
			}
		}
	}

	
}
