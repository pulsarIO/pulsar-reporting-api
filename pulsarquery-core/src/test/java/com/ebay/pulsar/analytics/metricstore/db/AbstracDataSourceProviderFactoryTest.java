/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.metricstore.db;

import static org.junit.Assert.*;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import com.ebay.pulsar.analytics.datasource.AbstractDataSourceProviderFactory;
import com.ebay.pulsar.analytics.datasource.DBConnector;
import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.DataSourceTypeEnum;
import com.ebay.pulsar.analytics.datasource.Table;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AbstracDataSourceProviderFactoryTest {
	@Test
	public void testFactory() {

		DBConnector collector=Mockito.mock(
				DBConnector.class);
		Table table=new Table();
		table.setTableName("tst");
		Mockito.when(collector.getAllTables()).thenReturn(Sets.newHashSet("tst"));
		Mockito.when(collector.getTableMeta("tst")).thenReturn(table);
		AbstractDataSourceProviderFactory factory = Mockito.mock(AbstractDataSourceProviderFactory.class,
				new CallsRealMethods());
		Mockito.doReturn(collector)
				.when(factory)
				.getDBCollector(Matchers.any(DataSourceConfiguration.class));
		DataSourceConfiguration configuration = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb");
		configuration.setEndPoint(Lists.newArrayList("http://test"));		assertEquals(Lists.newArrayList(table),factory.create(configuration).getTables());
	}

}
