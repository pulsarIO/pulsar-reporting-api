/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import com.ebay.pulsar.analytics.exception.DataSourceConfigurationException;
import com.ebay.pulsar.analytics.query.RestQueryProcessor;
import com.ebay.pulsar.analytics.query.SQLQueryProcessor;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarDataSourceProviderFactory extends AbstractDataSourceProviderFactory {
	
	private static PulsarDataSourceProviderFactory instance =  new PulsarDataSourceProviderFactory();
	public static PulsarDataSourceProviderFactory getInstance(){
		return instance;
	}
	
	private RestQueryProcessor restProcessor;
	
	private PulsarDataSourceProviderFactory(){
		DataSourceTypeRegistry.registerDataSourceType(DataSourceTypeEnum.PULSAR, PulsarDataSourceProviderFactory.class);
	}
	
	@Override
	public DataSourceProvider create(DataSourceConfiguration configuration) {
		DataSourceProvider db = super.create(configuration);
		return db;
	}
	
	@Override
	public DBConnector getDBCollector(DataSourceConfiguration configuration) {
		if(!(configuration instanceof PulsarDataSourceConfiguration)){
			throw new DataSourceConfigurationException("Error configuration!");
		}
		PulsarDataSourceConfiguration pulsarDataBaseConf = (PulsarDataSourceConfiguration)configuration;
		return new PulsarDataBaseConnector(pulsarDataBaseConf);
	}

	@Override
	public SQLQueryProcessor queryProcessor() {
		return restProcessor;
	}

	public RestQueryProcessor getRestProcessor() {
		return restProcessor;
	}

	public void setRestProcessor(RestQueryProcessor restProcessor) {
		this.restProcessor = restProcessor;
	}
}
