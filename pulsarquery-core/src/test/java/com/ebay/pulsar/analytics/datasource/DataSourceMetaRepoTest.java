/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.mockito.Mockito;

import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.service.DBDataSourceService;
import com.ebay.pulsar.analytics.datasource.loader.DataSourceConfigurationLoader;
import com.ebay.pulsar.analytics.datasource.loader.DynamicDataSourceConfigurationLoader;
import com.ebay.pulsar.analytics.datasource.loader.PeriodicalConfigurationLoader;
import com.ebay.pulsar.analytics.datasource.loader.StaticDataSourceConfigurationLoader;
import com.ebay.pulsar.analytics.metricstore.db.ReflectFieldUtil;
import com.google.common.collect.Lists;

public class DataSourceMetaRepoTest {
	private DataSourceMetaRepo metaRepo = DataSourceMetaRepo.getInstance();

	@Test
	public void testAddDbConf() {
		DataSourceConfiguration conf = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb");
		Properties pro = new Properties();
		pro.put("test", "test");
		conf.setEndPoint(Lists.newArrayList("testEndpoint"));
		conf.setProperties(pro);
		assertEquals("testdb", conf.getDataSourceName());
		assertEquals(DataSourceTypeEnum.DRUID, conf.getDataSourceType());
		metaRepo.addDbConf("testdb", conf);
		assertFalse(metaRepo.getActiveDbConfMap().containsKey("testdb"));
		assertFalse(metaRepo.getDbConfMap().containsKey("testdb"));

	}

	@Test
	public void testMetaCache() throws ExecutionException {
		DBConnector con = Mockito.mock(DBConnector.class);
		TableDimension dimension = new TableDimension();
		dimension.setName("testDim");
		dimension.setType(0);
		dimension.setMultiValue(true);

		DBConnector con2 = Mockito.mock(DBConnector.class);

		Table table = new Table();
		table.setTableName("testTable");
		table.setDimensions(Lists.newArrayList(dimension));
		table.setNoInnerJoin(false);
		table.setDateColumn("testDate");
		table.setMetrics(Lists.newArrayList(dimension));
		table.insertDimensionMap(dimension);
		table.insertMetricMap(dimension);

		Table table2 = new Table();
		table2.setTableName("testTable");

		assertEquals("testTable", table.getTableName());
		assertEquals(Lists.newArrayList(dimension), table.getDimensions());
		assertEquals("testDate", table.getDateColumn());
		assertEquals(dimension, table.getColumnMeta("testDim"));
		assertTrue(table.getColumnType("testDim") == 0);
		assertEquals(dimension, table.getDimensionByName("testDim"));
		assertEquals(dimension, table.getMetricByName("testDim"));
		assertEquals(true, table.isColumnMetric("testDim"));
		assertEquals(false, table.isColumnHyperLogLog("testDim"));
		assertEquals(false, table.isColumnNumeric("testDim"));
		assertEquals(false, table.isNoInnerJoin());

		List<Table> tables = new ArrayList<Table>();
		tables.add(table);

		DataSourceProvider provider = new DataSourceProvider();
		provider.setDataSourceName("testdb");
		provider.setConnector(con);
		provider.setTables(tables);
		assertEquals(con, provider.getConnector());
		assertEquals(tables, provider.getTables());

		DataSourceProvider provider2 = new DataSourceProvider();
		provider2.setDataSourceName("testdb");
		provider2.setConnector(con);
		provider2.setTables(tables);

		DataSourceProvider provider3 = new DataSourceProvider();
		provider3.setConnector(con);
		provider3.setTables(tables);

		DataSourceProvider provider4 = new DataSourceProvider();
		provider4.setDataSourceName("testdb");
		provider4.setTables(tables);
		provider4.setConnector(con2);

		DataSourceProvider provider5 = new DataSourceProvider();
		provider5.setDataSourceName("testdb");
		provider5.setConnector(con);
		provider5.setTables(Lists.newArrayList(table2));
		assertTrue(provider2.equals(provider));
		assertFalse(provider3.equals(provider));
		// assertFalse(provider4.equals(provider));
		assertFalse(provider5.equals(provider));
		assertTrue(provider.hashCode() == provider2.hashCode());

		// DataSourceMetaRepo metaRepoMock =
		// Mockito.mock(DataSourceMetaRepo.class,new CallsRealMethods());
		// Mockito.doReturn("").when(metaRepoMock).checkNameChange(Mockito.any(ColumnReference.class),Mockito.any(Table.class),Mockito.any(Map.class));
		//
		// final GeneratedKeyHolder keyHolder =
		// PowerMockito.mock(GeneratedKeyHolder.class);
		// PowerMockito.whenNew(GeneratedKeyHolder.class).withNoArguments()
		// .thenReturn(keyHolder);

		assertFalse(metaRepo.getAllDBMeta().containsKey("testdb"));
		assertFalse(metaRepo.getAllActiveDBMeta().containsKey("testdb"));
	}

	@Test
	public void testDBMetaFromCache() throws Exception {

		assertTrue(metaRepo.getAllDBMeta().size() == 0);
		assertTrue(metaRepo.getDBMetaFromCache("testdb") == null);
		metaRepo.disableDBMetaFromCache("testdb");
		assertTrue(metaRepo.getStats().size() > 0);
		metaRepo.disableMultiDBMetaFromCache(Lists.newArrayList("testdb"));
		assertTrue(metaRepo.getAllDBMeta().size() == 0);

		try {
			DataSourceConfigurationLoader staticLoader = new StaticDataSourceConfigurationLoader();
			staticLoader.load();
		} catch (Throwable ex) {
			ex.printStackTrace();
		}
		DBDataSourceService datasourceService = Mockito
				.mock(DBDataSourceService.class);
		DynamicDataSourceConfigurationLoader dynamicLoader = new DynamicDataSourceConfigurationLoader();

		DBDataSource datasource = new DBDataSource();
		datasource.setName("datasource");
		datasource.setId(1L);
		datasource.setOwner("user");
		datasource.setType("druid");
		datasource.setEndpoint("test");
		datasource.setLastUpdateTime(new Date());
		List<DBDataSource> sample = Lists.newArrayList(datasource);
		sample.add(datasource);

		ReflectFieldUtil.setField(DynamicDataSourceConfigurationLoader.class,
				dynamicLoader, "datasourceService", datasourceService);
		Mockito.when(datasourceService.getAll()).thenReturn(sample);

		try {
			DataSourceConfigurationLoader periodLoader = new PeriodicalConfigurationLoader(
					dynamicLoader);
			periodLoader.load();
		} catch (Throwable ex) {
			ex.printStackTrace();
		}

		assertTrue(metaRepo.getDbConfMap().size() == 0);

	}

	@Test
	public void testDataSourceType() {
		
		DataSourceTypeRegistry.registerDataSourceType(DataSourceTypeEnum.DRUID, DataSourceProviderFactory.class);
		DataSourceTypeRegistry.getDataSourceFactory(DataSourceTypeEnum.DRUID);
		assertTrue(DataSourceTypeRegistry.getAllSupportedDataSourceTypes()
				.size()>0);

	}

}
