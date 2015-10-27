/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import com.ebay.pulsar.analytics.metricstore.druid.query.DruidQueryProcessor;
import com.ebay.pulsar.analytics.query.SQLQueryProcessor;


/**
 * 
 * @author mingmwang
 *
 */
public class DruidDataSourceProviderFactory extends AbstractDataSourceProviderFactory {
	private final static DruidDataSourceProviderFactory instance =  new DruidDataSourceProviderFactory();
	public static DruidDataSourceProviderFactory getInstance(){
		return instance;
	}
	
	private SQLQueryProcessor processor = new DruidQueryProcessor();
	
	
	public SQLQueryProcessor getProcessor() {
		return processor;
	}

	public void setProcessor(SQLQueryProcessor processor) {
		this.processor = processor;
	}

	private DruidDataSourceProviderFactory(){
		DataSourceTypeRegistry.registerDataSourceType(DataSourceTypeEnum.DRUID, DruidDataSourceProviderFactory.class);
	}
	
	@Override
	public DBConnector getDBCollector(DataSourceConfiguration configuration) {
		return new DruidRestDBConnector(configuration);
	}

	@Override
	public SQLQueryProcessor queryProcessor() {
		return processor;
	}
}
