/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource.loader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.DataSourceTypeEnum;
import com.ebay.pulsar.analytics.exception.DataSourceConfigurationException;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * Load all the DB configurations from static configuration properties
 * 
 * @author mingmwang
 *
 */
public class StaticDataSourceConfigurationLoader implements DataSourceConfigurationLoader {
	private static final Logger logger = LoggerFactory.getLogger(StaticDataSourceConfigurationLoader.class);
	
	@Override
	public void load() {
		Enumeration<URL> urls = null;
		try {
			urls = Thread.currentThread().getContextClassLoader().getResources("datasources/");
		} catch (IOException e1) {
		}
		if (urls != null) {
			while (urls.hasMoreElements()) {
				URL url = urls.nextElement();
				File folder = new File(url.getFile());
				if (folder.isDirectory()) {
					File[] fileList = folder.listFiles();
					if(fileList != null){
						for (File f : fileList) {
							if (f.getName().endsWith("properties") || f.getName().endsWith("props")) {
								Properties properties = new Properties();
								FileReader fReader = null;
								try {
									fReader = new FileReader(f);
									properties.load(fReader);
									String datasourceType = (String) properties.get(DATASOURCE_TYPE);
									String databaseName = (String) properties.get(DATASOURCE_NAME);
									String endPoints = (String) properties.get(ENDPOINTS);
	
									properties.remove(DATASOURCE_TYPE);
									properties.remove(DATASOURCE_NAME);
									properties.remove(ENDPOINTS);
									
									DataSourceTypeEnum  dataSourceTypeEnum = DataSourceTypeEnum.fromType(datasourceType);
									if(dataSourceTypeEnum == null){
										throw new DataSourceConfigurationException("Unsupported dataSourceType:"+ datasourceType);
									}
									DataSourceConfiguration configuration = new DataSourceConfiguration(dataSourceTypeEnum, databaseName);
									configuration.setRealOnly(true);
									configuration.setEndPoint(Lists.newArrayList(Splitter.on(',').trimResults().split(endPoints)));
									configuration.setProperties(properties);
	
									logger.info("Add static metricstore configuration:" + databaseName);
									DataSourceMetaRepo.getInstance().addDbConf(databaseName, configuration);
								} catch (FileNotFoundException e) {
								} catch (IOException e) {
								}finally{
									if(fReader != null){
										try {
											fReader.close();
										} catch (IOException e) {
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
}
