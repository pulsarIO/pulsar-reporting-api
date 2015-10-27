/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

/**
 * 
 * @author mingmwang
 *
 */
public abstract class AbstractDataSourceProviderFactory implements DataSourceProviderFactory {
	private static final Logger logger = LoggerFactory.getLogger(AbstractDataSourceProviderFactory.class);

	public abstract DBConnector getDBCollector(DataSourceConfiguration configuration);
	
	@Override
	public boolean validate(DataSourceConfiguration configuration) {
		try{
			final DBConnector client = getDBCollector(configuration);
			Set<String> tableNames = client.getAllTables();
			if(tableNames.isEmpty()){
				logger.error("No tables found for this DataSource : "+ configuration.getDataSourceName());
				return false;
			}
			return true;
		}catch(Exception e){
			logger.error("DataSource Connection Error: "+ e);
			return false;
		}
	}
	
	@Override
	public DataSourceProvider create(DataSourceConfiguration configuration) {
		DataSourceProvider dataSourceProvider = new DataSourceProvider();
		dataSourceProvider.setDataSourceName(configuration.getDataSourceName());
		final DBConnector client = getDBCollector(configuration);
		
		Set<String> tableNames = client.getAllTables();
		
		List<Table> allTables = Lists.newArrayList(FluentIterable.from(tableNames)
				.transform(new Function<String, Table>() {
					@Override
					public Table apply(String input) {
						return client.getTableMeta(input);
					}
				}).filter(Predicates.notNull()));
		
		dataSourceProvider.setTables(allTables);
		dataSourceProvider.setConnector(client);
		return dataSourceProvider;
	}
}
