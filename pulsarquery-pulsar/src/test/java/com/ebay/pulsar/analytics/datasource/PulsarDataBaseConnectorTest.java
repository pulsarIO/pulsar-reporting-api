/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ebay.pulsar.analytics.query.request.DateRange;
import com.google.common.collect.Lists;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataSourceMetaRepo.class ,DruidDataSourceProviderFactory.class})
public class PulsarDataBaseConnectorTest {
	@Test
	public void testPulsarDataBaseConnector() {

		DataSourceProvider provider = Mockito.mock(DataSourceProvider.class);
		Table table=new Table();
		table.setTableName("event");
		TableDimension dimension=new TableDimension();
		dimension.setName("page");
		TableDimension metric=new TableDimension();
		metric.setName("pageview");
		table.setDimensions(Lists.newArrayList(dimension));
		table.setMetrics(Lists.newArrayList(metric));
		

		DruidDataSourceProviderFactory druidFactory = PowerMockito
				.mock(DruidDataSourceProviderFactory.class);
		PowerMockito.mockStatic(DruidDataSourceProviderFactory.class);

		Mockito.when(DruidDataSourceProviderFactory.getInstance()).thenReturn(
				druidFactory);
		when(druidFactory.create(Matchers.any(DataSourceConfiguration.class)))
				.thenReturn(provider);
		when(provider.getTableByName("event")).thenReturn(table);
		

		
		PowerMockito.mockStatic(DataSourceMetaRepo.class);
		DataSourceMetaRepo DataSourceMetaRepoIns = PowerMockito.mock(DataSourceMetaRepo.class);
		Mockito.when(DataSourceMetaRepo.getInstance()).thenReturn(DataSourceMetaRepoIns);
		
		DataSourceProvider lap = Mockito.mock(DataSourceProvider.class);
		Table lapt = Mockito.mock(Table.class);
		when(lap.getTableByName(Matchers.anyString())).thenReturn(lapt);
		when(lapt.getTableName()).thenReturn("event");
		when(DataSourceMetaRepoIns.getDBMetaFromCache("event")).thenReturn(lap);
				
		@SuppressWarnings("resource")
		ApplicationContext ctx = new ClassPathXmlApplicationContext(
				"datasourceconfig.xml");
		PulsarDataSourceConfiguration instance = (PulsarDataSourceConfiguration) ctx
				.getBean("PulsarDataSourceConfiguration");
		PulsarDataBaseConnector connect = new PulsarDataBaseConnector(instance);
		assertTrue("event".equals(connect.getTableMeta("event").getRTOLAPTableName()));
		
		PulsarTable pulsarTable=new PulsarTable();
		pulsarTable.setTableName("event");
		PulsarTableDimension pulsarDimension=new PulsarTableDimension();
		pulsarDimension.setName("page");
		PulsarTableDimension pulsarMetric=new PulsarTableDimension();
		pulsarMetric.setName("pageview");
		pulsarTable.setDimensions(Lists.newArrayList(pulsarDimension));
		pulsarTable.setMetrics(Lists.newArrayList(pulsarMetric));
		pulsarTable.setRTOLAPTableName("event");
		assertTrue("event".equals(connect.getTableMeta("event").getRTOLAPTableName()));
		
		Table table2=new Table();
		table2.setTableName("session");
		TableDimension dimension2=new TableDimension();
		dimension2.setName("page");
		TableDimension metric2=new TableDimension();
		metric2.setName("pageview");
		table2.setDimensions(Lists.newArrayList(dimension2));
		table2.setMetrics(Lists.newArrayList(metric2));
		
		Table table3=new Table();
		table.setTableName("pulsar_ogmb");
		TableDimension dimension3=new TableDimension();
		dimension3.setName("page");
		TableDimension metric3=new TableDimension();
		metric3.setName("pageview");
		table3.setDimensions(Lists.newArrayList(dimension3));
		table3.setMetrics(Lists.newArrayList(metric3));

		when(provider.getTableByName("session")).thenReturn(table2);
		when(provider.getTableByName("pulsar_ogmb")).thenReturn(table3);
		assertTrue(connect.getAllTables().contains("event"));
		DateTime startTime=DateTime.parse("2015-09-15T23:59:59");
		DateTime endTime=DateTime.parse("2015-09-15T23:59:59");
		DateRange dataRange = new DateRange(startTime, endTime);
		assertEquals("event",connect.getOLAPByTableAndIntervals("event", dataRange));
		
	}

}
