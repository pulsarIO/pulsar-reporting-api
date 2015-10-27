/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Lists;

public class DataSourceConfigurationTest {
	@Test
	public void testEquals(){

		DataSourceConfiguration conf = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb");
		DataSourceConfiguration conf2 = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb");
		Properties pro = new Properties();
		pro.put("test", "test");
		conf.setEndPoint(Lists.newArrayList("testEndpoint"));
		conf.setProperties(pro);
		conf.setDataSourceName("testdb");
		conf.setDataSourceType(DataSourceTypeEnum.DRUID);
		conf.setRealOnly(false);
		conf.setRefreshTime(12222222L);
		assertEquals(Lists.newArrayList("testEndpoint"),conf.getEndPoint());
		assertEquals(pro,conf.getProperties());
		assertEquals("testdb",conf.getDataSourceName());
		assertEquals(DataSourceTypeEnum.fromType("druid"),conf.getDataSourceType());
		assertEquals(12222222L,conf.getRefreshTime());
		

		conf2.setEndPoint(Lists.newArrayList("testEndpoint"));
		conf2.setProperties(pro);
		conf2.setDataSourceName("testdb");
		conf2.setDataSourceType(DataSourceTypeEnum.DRUID);
		conf2.setRealOnly(false);
		conf2.setRefreshTime(12222222L);
		DataSourceConfiguration conf3 = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb1");
//		DataSourceConfiguration conf4 = new DataSourceConfiguration(
//				DataSourceTypeEnum.KYLIN, "testdb");
		DataSourceConfiguration conf5 = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb");
		DataSourceConfiguration conf6 = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb");
		conf5.setEndPoint(Lists.newArrayList("testEndpoint2"));
		conf5.setProperties(pro);
		conf6.setEndPoint(Lists.newArrayList("testEndpoint"));
		
		DataSourceConfiguration conf7 = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb");		
		conf7.setProperties(pro);
		conf7.setDataSourceName("testdb");
		conf7.setDataSourceType(DataSourceTypeEnum.DRUID);
		conf7.setRealOnly(false);
		conf7.setRefreshTime(12222222L);
		
		DataSourceConfiguration conf8 = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb");		
		conf8.setEndPoint(Lists.newArrayList("testEndpoint"));
		conf8.setProperties(pro);
		conf8.setDataSourceName("testdb");
		conf8.setDataSourceType(DataSourceTypeEnum.DRUID);
		conf8.setRealOnly(false);

		
		DataSourceConfiguration conf9 = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, "testdb");		
		conf9.setEndPoint(Lists.newArrayList("testEndpoint"));
		conf9.setDataSourceName("testdb");
		conf9.setDataSourceType(DataSourceTypeEnum.DRUID);
		conf9.setRealOnly(false);
		conf9.setRefreshTime(12222222L);
		
		DataSourceConfiguration conf10 = new DataSourceConfiguration(
				DataSourceTypeEnum.DRUID, null);		
		conf10.setEndPoint(Lists.newArrayList("testEndpoint"));
		conf10.setProperties(pro);
		conf10.setDataSourceType(DataSourceTypeEnum.DRUID);
		conf10.setRefreshTime(12222222L);
		
		DataSourceConfiguration conf11 = new DataSourceConfiguration(
				null, "testdb");		
		conf11.setEndPoint(Lists.newArrayList("testEndpoint"));
		conf11.setProperties(pro);
		conf11.setDataSourceName("testdb");
		conf11.setRealOnly(false);
		conf11.setRefreshTime(12222222L);
		
		DataSourceConfiguration conf12 = new DataSourceConfiguration(
				null, "testdb");		
		conf12.setEndPoint(Lists.newArrayList("testEndpoint"));
		conf12.setProperties(pro);
		conf12.setDataSourceName("testdb");
		conf12.setRealOnly(false);
		conf12.setRefreshTime(12222222L);
		

		assertTrue(conf.equals(conf2));
		assertTrue(conf.hashCode()==conf2.hashCode());
		assertFalse(conf3.equals(conf));
//		assertFalse(conf4.equals(conf));
		assertFalse(conf5.equals(conf));
		assertFalse(conf6.equals(conf));
		assertFalse(conf7.equals(conf));
		assertFalse(conf8.equals(conf));
		assertFalse(conf9.equals(conf));
		assertFalse(conf10.equals(conf));
		assertFalse(conf11.equals(conf));
		assertFalse(conf12.equals(conf));
	}

}
