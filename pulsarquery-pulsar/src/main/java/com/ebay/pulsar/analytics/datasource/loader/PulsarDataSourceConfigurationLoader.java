/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.PulsarDataSourceConfiguration;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarDataSourceConfigurationLoader implements DataSourceConfigurationLoader {
	private static final Logger logger = LoggerFactory.getLogger(PulsarDataSourceConfigurationLoader.class);
	
	private PulsarDataSourceConfiguration configuration;
	
	public PulsarDataSourceConfigurationLoader(PulsarDataSourceConfiguration configuration){
		this.configuration = configuration;
	}

	@Override
	public void load() {
		logger.info("Add static metricstore configuration:" + PULSAR_DATASOURCE);
		DataSourceMetaRepo.getInstance().addDbConf(PULSAR_DATASOURCE, configuration);
	}
}
