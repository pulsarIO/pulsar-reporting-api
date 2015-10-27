/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;


import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * 
 * @author mingmwang
 *
 */
public class DataSourceMetaRepo {
	private static final Logger logger = LoggerFactory.getLogger(DataSourceMetaRepo.class);
	
	private ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

	private static volatile DataSourceMetaRepo instance = null;

	private final Map<String, DataSourceConfiguration> dbConfMap = Maps.newConcurrentMap();
	private final Map<String, DataSourceConfiguration> activeDbConfMap = Maps.newConcurrentMap();
	
	private final LoadingCache<String, DataSourceProvider> cache;
	
	// Cache Size
	private static int SIZE_CACHE = 10000;
	// Expire after 1 hours = 60 min
	private static final int TIME_EXPIRE = 60;
	// Refresh after 1 hours = 60 min
	private static final int TIME_REFRESH = 60;

	public static DataSourceMetaRepo getInstance() {
		if (instance == null) {
			synchronized (DataSourceMetaRepo.class) {
				if (instance == null) {
					instance = new DataSourceMetaRepo();
				}
			}
		}
		return instance;
	}
	
	public DataSourceConfiguration getDbConf(String datasourceName) {
		return dbConfMap.get(datasourceName.toLowerCase());
	}
	
	public void addDbConf(String datasourceName, DataSourceConfiguration conf) {
		DataSourceProviderFactory factory = DataSourceTypeRegistry.getDataSourceFactory(conf.getDataSourceType());
		if(factory == null){
			logger.error("Invalid DataSourceConfiguration: " + datasourceName);
			return;
		}
		if(factory.validate(conf)){
			dbConfMap.put(datasourceName.toLowerCase(), conf);
		}else{
			logger.error("Invalid DataSourceConfiguration: " + datasourceName);
		}
	}
	
	public Map<String, DataSourceProvider>  getAllDBMeta(){
		logger.info ("All MetricStore configuration size: " + dbConfMap.size());
		Map<String, DataSourceProvider>  tableMetas = getDBMetaFromCache(dbConfMap.keySet());
		logger.info ("Returned MetricStore instances: " + tableMetas.size());
		return tableMetas;
	}
	
	public Map<String, DataSourceProvider>  getAllActiveDBMeta(){
		logger.info ("Active MetricStore configuration size: " + activeDbConfMap.size());
		Map<String, DataSourceProvider>  tableMetas = getDBMetaFromCache(activeDbConfMap.keySet());
		logger.info ("Returned MetricStore instances: " + tableMetas.size());
		return tableMetas;
	}
	
	public Map<String, DataSourceConfiguration> getDbConfMap() {
		return dbConfMap;
	}

	public Map<String, DataSourceConfiguration> getActiveDbConfMap() {
		return activeDbConfMap;
	}

	private DataSourceMetaRepo() {
		cache = CacheBuilder.newBuilder()
					.maximumSize(SIZE_CACHE)
					.expireAfterWrite(TIME_EXPIRE, TimeUnit.MINUTES)
					.refreshAfterWrite(TIME_REFRESH, TimeUnit.MINUTES)
					.build( new CacheLoader<String, DataSourceProvider>() {
						@Override
						public DataSourceProvider load(String dataSourceName) throws Exception {
							return createDBInstance(dataSourceName);
						}

						@Override
						public Map<String, DataSourceProvider> loadAll(Iterable<? extends String> dataSourceNames) {
							return createMultiDBInstances(Lists.newArrayList(dataSourceNames));
						}

						@Override
						public ListenableFuture<DataSourceProvider> reload(final String dbNameSpace, DataSourceProvider prevDBMeta) {
							// asynchronous!
							ListenableFutureTask<DataSourceProvider> task = ListenableFutureTask.create(new Callable<DataSourceProvider>() {
																		public DataSourceProvider call() {
																			return createDBInstance(dbNameSpace);
																		}
																	});
							EXECUTOR.submit(task);
							return task;
						}
					});
	}


	public DataSourceProvider getDBMetaFromCache(String datasourceName) {
		DataSourceProvider dbMeta = null;
		try {
			dbMeta = cache.get(datasourceName.toLowerCase());
		} catch (Exception ex) {
			logger.error ("DataBaseMetaRepo getDBMetaFromCache Error: " + ex);
		}
		return dbMeta;
	}

	public Map<String, DataSourceProvider>  getDBMetaFromCache(Iterable<? extends String> datasourceNames) {
		Map<String, DataSourceProvider>  tableMetas = Maps.newHashMap();
		for(String datasourceName : datasourceNames){
			DataSourceProvider dbMeta = getDBMetaFromCache(datasourceName.toLowerCase());
			if(dbMeta != null){
				tableMetas.put(datasourceName.toLowerCase(), dbMeta);
			}
		}
		return tableMetas;
	}
	
	public void disableDBMetaFromCache(String datasourceName){
		cache.invalidate(datasourceName.toLowerCase());
		activeDbConfMap.remove(datasourceName.toLowerCase());
	}
	
	public void disableMultiDBMetaFromCache(Iterable<? extends String> datasourceNames){
		cache.invalidate(datasourceNames);
		for(String key : datasourceNames){
			activeDbConfMap.remove(key.toLowerCase());
		}
	}
	
	private DataSourceProvider createDBInstance (String dataSourceName) {
		DataSourceConfiguration configuration = dbConfMap.get(dataSourceName);
		if(configuration == null){
			return null;
		}
		DataSourceProviderFactory factory = DataSourceTypeRegistry.getDataSourceFactory(configuration.getDataSourceType());
		if(factory == null){
			return null;
		}
		DataSourceProvider dataBase = null;
		try{
			dataBase = factory.create(configuration);
		}catch(Exception e){
			logger.error ("create MetricStore error for dataSourceName:" + dataSourceName+ ", Error: " + e);
		}
		if(dataBase != null){
			activeDbConfMap.put(dataSourceName, dbConfMap.get(dataSourceName));
		}
		return dataBase;
	}

	private Map<String, DataSourceProvider> createMultiDBInstances (List<String> dataSourceNames) {
		if (dataSourceNames == null || dataSourceNames.size() == 0) {
			return null;
		}
		
		final Map<String, DataSourceProvider> tableMetasMap = Maps.newConcurrentMap();
		final CountDownLatch countDownLatch = new CountDownLatch(dataSourceNames.size());
		for (final String dbNameSpace : dataSourceNames) {
			EXECUTOR.submit(new Runnable() {
				@Override
				public void run() {
					DataSourceProvider dataBase = createDBInstance(dbNameSpace);
					if (dataBase != null) {
						tableMetasMap.put(dbNameSpace, dataBase);
					}
					countDownLatch.countDown();
				}
			});
		}
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
		}
		
		return tableMetasMap;
	}
	
	public Map<String, Object> getStats () {
		// Get Cache Stats: requestCount, hitRate
		Map<String, Object> map = Maps.newHashMap();
		CacheStats stats = cache.stats();

		Long reqCount = stats.requestCount();
		Double hitRate = stats.hitRate();
		map.put ("requestCount", reqCount);
		map.put ("hitRate", hitRate);
		return map;
	}
}
