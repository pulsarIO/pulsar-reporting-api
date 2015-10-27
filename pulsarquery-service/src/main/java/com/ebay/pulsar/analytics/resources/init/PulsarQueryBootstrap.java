/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.resources.init;

import java.util.List;

import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.PulsarDataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.loader.DataSourceConfigurationLoader;
import com.ebay.pulsar.analytics.datasource.loader.DynamicDataSourceConfigurationLoader;
import com.ebay.pulsar.analytics.datasource.loader.PeriodicalConfigurationLoader;
import com.ebay.pulsar.analytics.datasource.loader.PulsarDataSourceConfigurationLoader;
import com.ebay.pulsar.analytics.datasource.loader.StaticDataSourceConfigurationLoader;

/**
 * 
 * Bootstrap all the configurations of the processors and DB resources.
 * 
 * @author mingmwang
 */
public class PulsarQueryBootstrap {

	public PulsarQueryBootstrap(List<String> preLoadClazz, PulsarDataSourceConfiguration pulsarConfiguration) throws ClassNotFoundException {
		for(String clazz : preLoadClazz)
			Class.forName(clazz);
		initDBConfiguration(pulsarConfiguration);
	}
	
	private void initDBConfiguration(PulsarDataSourceConfiguration pulsarConfiguration) {
		try{
			DataSourceConfigurationLoader staticLoader = new StaticDataSourceConfigurationLoader();
			staticLoader.load();
		}catch(Throwable ex){
		}
		
		try{
			DataSourceConfigurationLoader pulsarLoader = new PulsarDataSourceConfigurationLoader(pulsarConfiguration);
			pulsarLoader.load();
		}catch(Throwable ex){
		}
		
		try{
			DataSourceConfigurationLoader periodLoader = new PeriodicalConfigurationLoader(new DynamicDataSourceConfigurationLoader());
			periodLoader.load();
		}catch(Throwable ex){
		}
		
		//early init all metadata
		DataSourceMetaRepo.getInstance().getAllDBMeta();
	}
}
