/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.metricstore.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Test;

import com.ebay.pulsar.analytics.dao.model.DBDashboard;
import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.model.DBGroup;
import com.ebay.pulsar.analytics.dao.model.DBRightGroup;
import com.ebay.pulsar.analytics.dao.model.DBTable;
import com.ebay.pulsar.analytics.dao.model.DBUser;
import com.ebay.pulsar.analytics.dao.model.DBUserGroup;

public class DBModelTest {
	@Test
	public void testDataSourceModel() throws ParseException{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		DBDataSource dbDataSource=new DBDataSource();
		dbDataSource.setId(0L);
		dbDataSource.setName("testDatasource");
		dbDataSource.setOwner("owner");
		dbDataSource.setType("druid");
		dbDataSource.setComment("comment");
		dbDataSource.setDisplayName("testDatasource");
		dbDataSource.setEndpoint("//");
		dbDataSource.setProperties("testPro");
		dbDataSource.setReadonly(false);
		dbDataSource.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource.setLastUpdateTime(formatter.parse("2015-09-15"));
		assertEquals("testDatasource",dbDataSource.getName());
		assertEquals("owner",dbDataSource.getOwner());
		assertEquals("comment",dbDataSource.getComment());
		assertEquals(false,dbDataSource.getReadonly());
		assertTrue(0L==dbDataSource.getId());
		assertEquals("testDatasource",dbDataSource.getDisplayName());
		assertEquals("druid",dbDataSource.getType());
		assertEquals("//",dbDataSource.getEndpoint());
		assertEquals("testPro",dbDataSource.getProperties());
		assertEquals(formatter.parse("2015-09-13"),dbDataSource.getCreateTime());
		assertEquals(formatter.parse("2015-09-15"),dbDataSource.getLastUpdateTime());

		DBDataSource dbDataSource2=new DBDataSource();
		dbDataSource2.setId(0L);
		dbDataSource2.setName("testDatasource");
		dbDataSource2.setOwner("owner");
		dbDataSource2.setType("druid");
		dbDataSource2.setComment("comment");
		dbDataSource2.setDisplayName("testDatasource");
		dbDataSource2.setEndpoint("//");
		dbDataSource2.setProperties("testPro");
		dbDataSource2.setReadonly(false);
		dbDataSource2.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource2.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource3=new DBDataSource();
		dbDataSource3.setName("testDatasource");
		dbDataSource3.setOwner("owner");
		dbDataSource3.setType("druid");
		dbDataSource3.setComment("comment");
		dbDataSource3.setEndpoint("//");
		dbDataSource3.setProperties("testPro");
		dbDataSource3.setReadonly(false);
		dbDataSource3.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource3.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource4=new DBDataSource();
		dbDataSource4.setName("testDatasource");
		dbDataSource4.setOwner("owner");
		dbDataSource4.setType("druid");
		dbDataSource4.setDisplayName("testDatasource");
		dbDataSource4.setEndpoint("//");
		dbDataSource4.setProperties("testPro");
		dbDataSource4.setReadonly(false);
		dbDataSource4.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource4.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource5=new DBDataSource();
		dbDataSource5.setName("testDatasource");
		dbDataSource5.setOwner("owner");
		dbDataSource5.setType("druid");
		dbDataSource5.setComment("comment");
		dbDataSource5.setDisplayName("testDatasource");
		dbDataSource5.setProperties("testPro");
		dbDataSource5.setReadonly(false);
		dbDataSource5.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource5.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource6=new DBDataSource();
		dbDataSource6.setName("testDatasource");
		dbDataSource6.setOwner("owner");
		dbDataSource6.setType("druid");
		dbDataSource6.setComment("comment");
		dbDataSource6.setDisplayName("testDatasource");
		dbDataSource6.setEndpoint("//");
		dbDataSource6.setReadonly(false);
		dbDataSource6.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource6.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource7=new DBDataSource();
		dbDataSource7.setName("testDatasource");
		dbDataSource7.setOwner("owner");
		dbDataSource7.setType("druid");
		dbDataSource7.setComment("comment7");
		dbDataSource7.setDisplayName("testDatasource7");
		dbDataSource7.setEndpoint("//ip");
		dbDataSource7.setProperties("testPro7");
		dbDataSource7.setReadonly(false);
		dbDataSource7.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource7.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource8=new DBDataSource();
		dbDataSource8.setId(0L);
		dbDataSource8.setOwner("owner");
		dbDataSource8.setType("druid");
		dbDataSource8.setComment("comment");
		dbDataSource8.setDisplayName("testDatasource");
		dbDataSource8.setEndpoint("//");
		dbDataSource8.setProperties("testPro");
		dbDataSource8.setReadonly(false);
		dbDataSource8.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource8.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource9=new DBDataSource();
		dbDataSource9.setId(0L);
		dbDataSource9.setName("testDatasource");
		dbDataSource9.setType("druid");
		dbDataSource9.setComment("comment");
		dbDataSource9.setDisplayName("testDatasource");
		dbDataSource9.setEndpoint("//");
		dbDataSource9.setProperties("testPro");
		dbDataSource9.setReadonly(false);
		dbDataSource9.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource9.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource10=new DBDataSource();
		dbDataSource10.setId(0L);
		dbDataSource10.setName("testDatasource");
		dbDataSource10.setOwner("owner");
		dbDataSource10.setType("druid");
		dbDataSource10.setComment("comment");
		dbDataSource10.setDisplayName("testDatasource");
		dbDataSource10.setEndpoint("//");
		dbDataSource10.setReadonly(false);
		dbDataSource10.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource10.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		
		DBDataSource dbDataSource11=new DBDataSource();
		dbDataSource11.setId(0L);
		dbDataSource11.setName("testDatasource");
		dbDataSource11.setOwner("owner");
		dbDataSource11.setType("druid");
		dbDataSource11.setComment("comment");
		dbDataSource11.setDisplayName("testDatasource");
		dbDataSource11.setEndpoint("//");
		dbDataSource11.setProperties("testPro");
		dbDataSource11.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource11.setLastUpdateTime(formatter.parse("2015-09-15"));

		
		DBDataSource dbDataSource12=new DBDataSource();
		dbDataSource12.setId(1L);
		dbDataSource12.setName("testDatasource");
		dbDataSource12.setOwner("owner");
		dbDataSource12.setType("druid");
		dbDataSource12.setComment("comment");
		dbDataSource12.setDisplayName("testDatasource");
		dbDataSource12.setEndpoint("//");
		dbDataSource12.setProperties("testPro");
		dbDataSource12.setReadonly(false);
		dbDataSource12.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource12.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource13=new DBDataSource();
		dbDataSource13.setId(0L);
		dbDataSource13.setName("testDatasource13");
		dbDataSource13.setOwner("owner");
		dbDataSource13.setType("druid");
		dbDataSource13.setComment("comment");
		dbDataSource13.setDisplayName("testDatasource");
		dbDataSource13.setEndpoint("//");
		dbDataSource13.setProperties("testPro");
		dbDataSource13.setReadonly(false);
		dbDataSource13.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource13.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource14=new DBDataSource();
		dbDataSource14.setId(0L);
		dbDataSource14.setName("testDatasource");
		dbDataSource14.setOwner("owner14");
		dbDataSource14.setType("druid");
		dbDataSource14.setComment("comment");
		dbDataSource14.setDisplayName("testDatasource");
		dbDataSource14.setEndpoint("//");
		dbDataSource14.setProperties("testPro");
		dbDataSource14.setReadonly(false);
		dbDataSource14.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource14.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource15=new DBDataSource();
		dbDataSource15.setId(0L);
		dbDataSource15.setName("testDatasource");
		dbDataSource15.setOwner("owner");
		dbDataSource15.setType("kylin");
		dbDataSource15.setComment("comment");
		dbDataSource15.setDisplayName("testDatasource");
		dbDataSource15.setEndpoint("//");
		dbDataSource15.setProperties("testPro");
		dbDataSource15.setReadonly(false);
		dbDataSource15.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource15.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource16=new DBDataSource();
		dbDataSource16.setId(0L);
		dbDataSource16.setName("testDatasource");
		dbDataSource16.setOwner("owner");
		dbDataSource16.setType("druid");
		dbDataSource16.setComment("comment16");
		dbDataSource16.setDisplayName("testDatasource");
		dbDataSource16.setEndpoint("//");
		dbDataSource16.setProperties("testPro");
		dbDataSource16.setReadonly(false);
		dbDataSource16.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource16.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource17=new DBDataSource();
		dbDataSource17.setId(0L);
		dbDataSource17.setName("testDatasource");
		dbDataSource17.setOwner("owner");
		dbDataSource17.setType("druid");
		dbDataSource17.setComment("comment");
		dbDataSource17.setDisplayName("testDatasource17");
		dbDataSource17.setEndpoint("//");
		dbDataSource17.setProperties("testPro");
		dbDataSource17.setReadonly(false);
		dbDataSource17.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource17.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource18=new DBDataSource();
		dbDataSource18.setId(0L);
		dbDataSource18.setName("testDatasource");
		dbDataSource18.setOwner("owner");
		dbDataSource18.setType("druid");
		dbDataSource18.setComment("comment");
		dbDataSource18.setDisplayName("testDatasource17");
		dbDataSource18.setEndpoint("//18");
		dbDataSource18.setProperties("testPro");
		dbDataSource18.setReadonly(false);
		dbDataSource18.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource18.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource19=new DBDataSource();
		dbDataSource19.setId(0L);
		dbDataSource19.setName("testDatasource");
		dbDataSource19.setOwner("owner");
		dbDataSource19.setType("druid");
		dbDataSource19.setComment("comment");
		dbDataSource19.setDisplayName("testDatasource17");
		dbDataSource19.setEndpoint("//");
		dbDataSource19.setProperties("testPro19");
		dbDataSource19.setReadonly(false);
		dbDataSource19.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource19.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource20=new DBDataSource();
		dbDataSource20.setId(0L);
		dbDataSource20.setName("testDatasource");
		dbDataSource20.setOwner("owner");
		dbDataSource20.setType("druid");
		dbDataSource20.setComment("comment");
		dbDataSource20.setDisplayName("testDatasource17");
		dbDataSource20.setEndpoint("//");
		dbDataSource20.setProperties("testPro");
		dbDataSource20.setReadonly(true);
		dbDataSource20.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource20.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource21=new DBDataSource();
		dbDataSource21.setId(0L);
		dbDataSource21.setName("testDatasource");
		dbDataSource21.setOwner("owner");
		dbDataSource21.setType("druid");
		dbDataSource21.setComment("comment");
		dbDataSource21.setDisplayName("testDatasource17");
		dbDataSource21.setEndpoint("//");
		dbDataSource21.setProperties("testPro");
		dbDataSource21.setReadonly(false);
		dbDataSource21.setCreateTime(formatter.parse("2015-09-14"));
		dbDataSource21.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource22=new DBDataSource();
		dbDataSource22.setId(0L);
		dbDataSource22.setName("testDatasource");
		dbDataSource22.setOwner("owner");
		dbDataSource22.setType("druid");
		dbDataSource22.setComment("comment");
		dbDataSource22.setDisplayName("testDatasource17");
		dbDataSource22.setEndpoint("//");
		dbDataSource22.setProperties("testPro");
		dbDataSource22.setReadonly(false);
		dbDataSource22.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource22.setLastUpdateTime(formatter.parse("2015-09-14"));
		
		DBDataSource dbDataSource23=new DBDataSource();
		dbDataSource23.setId(0L);
		dbDataSource23.setName("testDatasource");
		dbDataSource23.setOwner("owner");
		dbDataSource23.setType("druid");
		dbDataSource23.setComment("comment");
		dbDataSource23.setDisplayName("testDatasource");
		dbDataSource23.setEndpoint("//");
		dbDataSource23.setProperties("testPro23");
		dbDataSource23.setReadonly(false);
		dbDataSource23.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource23.setLastUpdateTime(formatter.parse("2015-09-14"));
		
		DBDataSource dbDataSource24=new DBDataSource();
		dbDataSource24.setId(0L);
		dbDataSource24.setName("testDatasource");
		dbDataSource24.setOwner("owner");
		dbDataSource24.setComment("comment");
		dbDataSource24.setDisplayName("testDatasource");
		dbDataSource24.setEndpoint("//");
		dbDataSource24.setProperties("testPro");
		dbDataSource24.setReadonly(false);
		dbDataSource24.setCreateTime(formatter.parse("2015-09-13"));
		dbDataSource24.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource25=new DBDataSource();
		dbDataSource25.setId(0L);
		dbDataSource25.setName("testDatasource");
		dbDataSource25.setOwner("owner");
		dbDataSource25.setType("druid");
		dbDataSource25.setComment("comment");
		dbDataSource25.setDisplayName("testDatasource");
		dbDataSource25.setEndpoint("//");
		dbDataSource25.setProperties("testPro");
		dbDataSource25.setReadonly(false);
		dbDataSource25.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDataSource dbDataSource26=new DBDataSource();
		dbDataSource26.setId(0L);
		dbDataSource26.setName("testDatasource");
		dbDataSource26.setOwner("owner");
		dbDataSource26.setType("druid");
		dbDataSource26.setComment("comment");
		dbDataSource26.setDisplayName("testDatasource");
		dbDataSource26.setEndpoint("//");
		dbDataSource26.setProperties("testPro");
		dbDataSource26.setReadonly(false);
		dbDataSource26.setCreateTime(formatter.parse("2015-09-13"));

		assertTrue(dbDataSource2.equals(dbDataSource));
		assertFalse(dbDataSource3.equals(dbDataSource));
		assertFalse(dbDataSource4.equals(dbDataSource));
		assertFalse(dbDataSource5.equals(dbDataSource));
		assertFalse(dbDataSource6.equals(dbDataSource));
		assertFalse(dbDataSource7.equals(dbDataSource));
		assertFalse(dbDataSource8.equals(dbDataSource));
		assertFalse(dbDataSource9.equals(dbDataSource));
		assertFalse(dbDataSource10.equals(dbDataSource));
		assertFalse(dbDataSource11.equals(dbDataSource));
		assertFalse(dbDataSource12.equals(dbDataSource));
		assertFalse(dbDataSource13.equals(dbDataSource));
		assertFalse(dbDataSource14.equals(dbDataSource));
		assertFalse(dbDataSource15.equals(dbDataSource));
		assertFalse(dbDataSource16.equals(dbDataSource));
		assertFalse(dbDataSource17.equals(dbDataSource));
		assertFalse(dbDataSource18.equals(dbDataSource));
		assertFalse(dbDataSource19.equals(dbDataSource));
		assertFalse(dbDataSource20.equals(dbDataSource));
		assertFalse(dbDataSource21.equals(dbDataSource));
		assertFalse(dbDataSource22.equals(dbDataSource));
		assertFalse(dbDataSource23.equals(dbDataSource));
		assertFalse(dbDataSource24.equals(dbDataSource));
		assertFalse(dbDataSource25.equals(dbDataSource));
		assertFalse(dbDataSource26.equals(dbDataSource));
		assertTrue(dbDataSource.hashCode()==dbDataSource2.hashCode());
	}
	
	@Test
	public void testDashboardModel() throws ParseException{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		DBDashboard dbDashboard=new DBDashboard();
		dbDashboard.setId(0L);
		dbDashboard.setName("testDatasource");
		dbDashboard.setOwner("owner");
		dbDashboard.setConfig("config");
		dbDashboard.setDisplayName("testDatasource");
		dbDashboard.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard.setLastUpdateTime(formatter.parse("2015-09-15"));
		assertEquals("testDatasource",dbDashboard.getName());
		assertEquals("owner",dbDashboard.getOwner());
		assertEquals("config",dbDashboard.getConfig());
		assertTrue(0L==dbDashboard.getId());
		assertEquals("testDatasource",dbDashboard.getDisplayName());
		assertEquals(formatter.parse("2015-09-13"),dbDashboard.getCreateTime());
		assertEquals(formatter.parse("2015-09-15"),dbDashboard.getLastUpdateTime());

		DBDashboard dbDashboard2=new DBDashboard();
		dbDashboard2.setId(0L);
		dbDashboard2.setName("testDatasource");
		dbDashboard2.setOwner("owner");
		dbDashboard2.setConfig("config");
		dbDashboard2.setDisplayName("testDatasource");
		dbDashboard2.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard2.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard3=new DBDashboard();
		dbDashboard3.setId(0L);
		dbDashboard3.setName("testDatasource");
		dbDashboard3.setOwner("owner");
		dbDashboard3.setConfig("config");
		dbDashboard3.setDisplayName("testDatasource");
		dbDashboard3.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard3.setLastUpdateTime(formatter.parse("2015-09-14"));
		
		DBDashboard dbDashboard4=new DBDashboard();
		dbDashboard4.setId(0L);
		dbDashboard4.setOwner("owner");
		dbDashboard4.setConfig("config");
		dbDashboard4.setDisplayName("testDatasource");
		dbDashboard4.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard4.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard5=new DBDashboard();
		dbDashboard5.setId(0L);
		dbDashboard5.setName("testDatasource");
		dbDashboard5.setConfig("config");
		dbDashboard5.setDisplayName("testDatasource");
		dbDashboard5.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard5.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard6=new DBDashboard();
		dbDashboard6.setId(0L);
		dbDashboard6.setName("testDatasource");
		dbDashboard6.setOwner("owner");
		dbDashboard6.setConfig("config");
		dbDashboard6.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard6.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard7=new DBDashboard();
		dbDashboard7.setId(0L);
		dbDashboard7.setName("testDatasource");
		dbDashboard7.setOwner("owner");
		dbDashboard7.setConfig("config");
		dbDashboard7.setDisplayName("testDatasource");
		dbDashboard7.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard8=new DBDashboard();
		dbDashboard8.setId(0L);
		dbDashboard8.setName("testDatasource");
		dbDashboard8.setOwner("owner");
		dbDashboard8.setConfig("config");
		dbDashboard8.setDisplayName("testDatasource");
		dbDashboard8.setCreateTime(formatter.parse("2015-09-13"));
		
		DBDashboard dbDashboard9=new DBDashboard();
		dbDashboard9.setId(1L);
		dbDashboard9.setName("testDatasource");
		dbDashboard9.setOwner("owner");
		dbDashboard9.setConfig("config");
		dbDashboard9.setDisplayName("testDatasource");
		dbDashboard9.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard9.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard10=new DBDashboard();
		dbDashboard10.setId(0L);
		dbDashboard10.setName("testDatasource10");
		dbDashboard10.setOwner("owner");
		dbDashboard10.setConfig("config");
		dbDashboard10.setDisplayName("testDatasource");
		dbDashboard10.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard10.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard11=new DBDashboard();
		dbDashboard11.setId(0L);
		dbDashboard11.setName("testDatasource");
		dbDashboard11.setOwner("owner11");
		dbDashboard11.setConfig("config");
		dbDashboard11.setDisplayName("testDatasource");
		dbDashboard11.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard11.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard12=new DBDashboard();
		dbDashboard12.setId(0L);
		dbDashboard12.setName("testDatasource");
		dbDashboard12.setOwner("owner");
		dbDashboard12.setConfig("config12");
		dbDashboard12.setDisplayName("testDatasource");
		dbDashboard12.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard12.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard13=new DBDashboard();
		dbDashboard13.setId(0L);
		dbDashboard13.setName("testDatasource");
		dbDashboard13.setOwner("owner");
		dbDashboard13.setConfig("config");
		dbDashboard13.setDisplayName("testDatasource13");
		dbDashboard13.setCreateTime(formatter.parse("2015-09-13"));
		dbDashboard13.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBDashboard dbDashboard14=new DBDashboard();
		dbDashboard14.setId(0L);
		dbDashboard14.setName("testDatasource");
		dbDashboard14.setOwner("owner");
		dbDashboard14.setConfig("config");
		dbDashboard14.setDisplayName("testDatasource");
		dbDashboard14.setCreateTime(formatter.parse("2015-09-14"));
		dbDashboard14.setLastUpdateTime(formatter.parse("2015-09-15"));
		

		assertTrue(dbDashboard2.equals(dbDashboard));
		assertFalse(dbDashboard3.equals(dbDashboard));
		assertFalse(dbDashboard4.equals(dbDashboard));
		assertFalse(dbDashboard5.equals(dbDashboard));
		assertFalse(dbDashboard6.equals(dbDashboard));
		assertFalse(dbDashboard7.equals(dbDashboard));
		assertFalse(dbDashboard8.equals(dbDashboard));
		assertFalse(dbDashboard9.equals(dbDashboard));
		assertFalse(dbDashboard10.equals(dbDashboard));
		assertFalse(dbDashboard11.equals(dbDashboard));
		assertFalse(dbDashboard12.equals(dbDashboard));
		assertFalse(dbDashboard13.equals(dbDashboard));
		assertFalse(dbDashboard14.equals(dbDashboard));
		assertTrue(dbDashboard2.hashCode()==dbDashboard.hashCode());
	}
	
	@Test
	public void testGroupModel() throws ParseException{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		DBGroup dbGroup=new DBGroup();
		dbGroup.setName("testGroup");
		dbGroup.setOwner("owner");
		dbGroup.setComment("comment");
		dbGroup.setId(0L);
		dbGroup.setDisplayName("testGroup");
		dbGroup.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup.setLastUpdateTime(formatter.parse("2015-09-15"));
		assertEquals("testGroup",dbGroup.getName());
		assertEquals("owner",dbGroup.getOwner());
		assertEquals("comment",dbGroup.getComment());
		assertTrue(0L==dbGroup.getId());
		assertEquals("testGroup",dbGroup.getDisplayName());
		assertEquals(formatter.parse("2015-09-14"),dbGroup.getCreateTime());
		assertEquals(formatter.parse("2015-09-15"),dbGroup.getLastUpdateTime());

		DBGroup dbGroup2=new DBGroup();
		dbGroup2.setName("testGroup");
		dbGroup2.setOwner("owner");
		dbGroup2.setComment("comment");
		dbGroup2.setId(0L);
		dbGroup2.setDisplayName("testGroup");
		dbGroup2.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup2.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup3=new DBGroup();
		dbGroup3.setName("testGroup");
		dbGroup3.setOwner("owner");
		dbGroup3.setComment("comment");
		dbGroup3.setId(1L);
		dbGroup3.setDisplayName("testGroup");
		dbGroup3.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup3.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup4=new DBGroup();
		dbGroup4.setName("testGroup");
		dbGroup4.setOwner("owner");
		dbGroup4.setComment("comment");
		dbGroup4.setId(0L);
		dbGroup4.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup4.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup5=new DBGroup();
		dbGroup5.setName("testGroup");
		dbGroup5.setOwner("owner");
		dbGroup5.setComment("comment");
		dbGroup5.setDisplayName("testGroup");
		dbGroup5.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup5.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup6=new DBGroup();
		dbGroup6.setName("testGroup");
		dbGroup6.setOwner("owner");
		dbGroup6.setId(0L);
		dbGroup6.setDisplayName("testGroup");
		dbGroup6.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup6.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup7=new DBGroup();
		dbGroup7.setName("testGroup");
		dbGroup7.setComment("comment");
		dbGroup7.setId(0L);
		dbGroup7.setDisplayName("testGroup");
		dbGroup7.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup7.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup8=new DBGroup();
		dbGroup8.setName("testGroup");
		dbGroup8.setOwner("owner");
		dbGroup8.setComment("comment");
		dbGroup8.setId(0L);
		dbGroup8.setDisplayName("testGroup");
		dbGroup8.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup9=new DBGroup();
		dbGroup9.setName("testGroup");
		dbGroup9.setOwner("owner");
		dbGroup9.setComment("comment");
		dbGroup9.setId(0L);
		dbGroup9.setDisplayName("testGroup");
		dbGroup9.setCreateTime(formatter.parse("2015-09-14"));
		
		DBGroup dbGroup10=new DBGroup();
		dbGroup10.setName("testGroup10");
		dbGroup10.setOwner("owner");
		dbGroup10.setComment("comment");
		dbGroup10.setId(0L);
		dbGroup10.setDisplayName("testGroup");
		dbGroup10.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup10.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup11=new DBGroup();
		dbGroup11.setName("testGroup");
		dbGroup11.setOwner("owner11");
		dbGroup11.setComment("comment");
		dbGroup11.setId(0L);
		dbGroup11.setDisplayName("testGroup");
		dbGroup11.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup11.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup12=new DBGroup();
		dbGroup12.setName("testGroup");
		dbGroup12.setOwner("owner");
		dbGroup12.setComment("comment12");
		dbGroup12.setId(0L);
		dbGroup12.setDisplayName("testGroup");
		dbGroup12.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup12.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup13=new DBGroup();
		dbGroup13.setName("testGroup");
		dbGroup13.setOwner("owner");
		dbGroup13.setComment("comment");
		dbGroup13.setId(0L);
		dbGroup13.setDisplayName("testGroup13");
		dbGroup13.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup13.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup14=new DBGroup();
		dbGroup14.setName("testGroup");
		dbGroup14.setOwner("owner");
		dbGroup14.setComment("comment");
		dbGroup14.setId(0L);
		dbGroup14.setDisplayName("testGroup");
		dbGroup14.setCreateTime(formatter.parse("2015-09-13"));
		dbGroup14.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBGroup dbGroup15=new DBGroup();
		dbGroup15.setName("testGroup");
		dbGroup15.setOwner("owner");
		dbGroup15.setComment("comment");
		dbGroup15.setId(0L);
		dbGroup15.setDisplayName("testGroup");
		dbGroup15.setCreateTime(formatter.parse("2015-09-14"));
		dbGroup15.setLastUpdateTime(formatter.parse("2015-09-13"));
		
		
		assertTrue(dbGroup2.equals(dbGroup));
		assertFalse(dbGroup3.equals(dbGroup));
		assertFalse(dbGroup4.equals(dbGroup));
		assertFalse(dbGroup5.equals(dbGroup));
		assertFalse(dbGroup6.equals(dbGroup));
		assertFalse(dbGroup7.equals(dbGroup));
		assertFalse(dbGroup8.equals(dbGroup));
		assertFalse(dbGroup9.equals(dbGroup));
		assertFalse(dbGroup10.equals(dbGroup));
		assertFalse(dbGroup11.equals(dbGroup));
		assertFalse(dbGroup12.equals(dbGroup));
		assertFalse(dbGroup13.equals(dbGroup));
		assertFalse(dbGroup14.equals(dbGroup));
		assertFalse(dbGroup15.equals(dbGroup));
		assertTrue(dbGroup.hashCode()==dbGroup2.hashCode());
	}
	
	@Test
	public void testUserModel() throws ParseException{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		DBUser dbUser=new DBUser();
		dbUser.setName("testUser");
		dbUser.setEmail("testEmial");
		dbUser.setComment("comment");
		dbUser.setEnabled(false);
		dbUser.setId(0L);
		dbUser.setImage("testImage");
		dbUser.setPassword("password");
		dbUser.setCreateTime(formatter.parse("2015-09-13"));
		dbUser.setLastUpdateTime(formatter.parse("2015-09-15"));
		assertEquals("testUser",dbUser.getName());
		assertEquals("testEmial",dbUser.getEmail());
		assertEquals("comment",dbUser.getComment());
		assertEquals(false,dbUser.getEnabled());
		assertTrue(0L==dbUser.getId());
		assertEquals("testImage",dbUser.getImage());
		assertEquals("password",dbUser.getPassword());
		assertEquals(formatter.parse("2015-09-13"),dbUser.getCreateTime());
		assertEquals(formatter.parse("2015-09-15"),dbUser.getLastUpdateTime());
		
		DBUser dbUser1=new DBUser();
		dbUser1.setName("testUser");
		dbUser1.setEmail("testEmial");
		dbUser1.setComment("comment");
		dbUser1.setEnabled(false);
		dbUser1.setId(0L);
		dbUser1.setImage("testImage");
		dbUser1.setPassword("password");
		dbUser1.setCreateTime(formatter.parse("2015-09-13"));
		dbUser1.setLastUpdateTime(formatter.parse("2015-09-15"));

		DBUser dbUser2=new DBUser();
		dbUser2.setName("testUser");
		dbUser2.setEmail("testEmial");
		dbUser2.setComment("comment");
		dbUser2.setEnabled(false);
		dbUser2.setImage("testImage");
		dbUser2.setPassword("password");
		dbUser2.setCreateTime(formatter.parse("2015-09-13"));
		dbUser2.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser3=new DBUser();
		dbUser.setName("testUser");
		dbUser3.setEmail("testEmial3");
		dbUser3.setComment("comment");
		dbUser3.setEnabled(false);
		dbUser3.setId(0L);
		dbUser3.setImage("testImage");
		dbUser3.setPassword("password");
		dbUser3.setCreateTime(formatter.parse("2015-09-13"));
		dbUser3.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser4=new DBUser();
		dbUser4.setName("testUser");
		dbUser4.setComment("comment");
		dbUser4.setEnabled(false);
		dbUser4.setId(0L);
		dbUser4.setImage("testImage");
		dbUser4.setCreateTime(formatter.parse("2015-09-13"));
		dbUser4.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser5=new DBUser();
		dbUser5.setName("testUser");
		dbUser5.setEmail("testEmial");
		dbUser5.setEnabled(false);
		dbUser5.setId(0L);
		dbUser5.setImage("testImage");
		dbUser5.setPassword("password");
		dbUser5.setCreateTime(formatter.parse("2015-09-13"));

		
		DBUser dbUser6=new DBUser();
		dbUser6.setName("testUser");
		dbUser6.setEmail("testEmial");
		dbUser6.setComment("comment");
		dbUser6.setId(0L);
		dbUser6.setImage("testImage");
		dbUser6.setPassword("password");
		dbUser6.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser7=new DBUser();
		dbUser7.setName("testUser");
		dbUser7.setEmail("testEmial");
		dbUser7.setComment("comment");
		dbUser7.setEnabled(false);
		dbUser7.setId(0L);
		dbUser7.setPassword("password");
		dbUser7.setCreateTime(formatter.parse("2015-09-13"));
		
		DBUser dbUser8=new DBUser();
		dbUser8.setName("testUser");
		dbUser8.setEmail("testEmial2");
		dbUser8.setComment("comment");
		dbUser8.setEnabled(false);
		dbUser8.setId(0L);
		dbUser8.setImage("testImage");
		dbUser8.setPassword("password");
		dbUser8.setCreateTime(formatter.parse("2015-09-13"));
		
		DBUser dbUser9=new DBUser();
		dbUser9.setName("testUser");
		dbUser9.setEmail("testEmial");
		dbUser9.setComment("comment");
		dbUser9.setEnabled(false);
		dbUser9.setId(0L);
		dbUser9.setImage("testImage");
		dbUser9.setPassword("password");
		dbUser9.setCreateTime(formatter.parse("2015-09-13"));
		
		DBUser dbUser10=new DBUser();
		dbUser10.setEmail("testEmial");
		dbUser10.setComment("comment");
		dbUser10.setEnabled(false);
		dbUser10.setId(0L);
		dbUser10.setImage("testImage");
		dbUser10.setPassword("password");
		dbUser10.setCreateTime(formatter.parse("2015-09-13"));
		dbUser10.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser11=new DBUser();
		dbUser11.setName("testUser");
		dbUser11.setEmail("testEmial");
		dbUser11.setComment("comment");
		dbUser11.setEnabled(false);
		dbUser11.setId(0L);
		dbUser11.setImage("testImage");
		dbUser11.setCreateTime(formatter.parse("2015-09-13"));
		dbUser11.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser12=new DBUser();
		dbUser12.setName("testUser12");
		dbUser12.setEmail("testEmial");
		dbUser12.setComment("comment");
		dbUser12.setEnabled(false);
		dbUser12.setId(0L);
		dbUser12.setImage("testImage");
		dbUser12.setPassword("password");
		dbUser12.setCreateTime(formatter.parse("2015-09-13"));
		dbUser12.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser13=new DBUser();
		dbUser13.setName("testUser");
		dbUser13.setEmail("testEmial13");
		dbUser13.setComment("comment");
		dbUser13.setEnabled(false);
		dbUser13.setId(0L);
		dbUser13.setImage("testImage");
		dbUser13.setPassword("password");
		dbUser13.setCreateTime(formatter.parse("2015-09-13"));
		dbUser13.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser14=new DBUser();
		dbUser14.setName("testUser");
		dbUser14.setEmail("testEmial");
		dbUser14.setComment("comment14");
		dbUser14.setEnabled(false);
		dbUser14.setId(0L);
		dbUser14.setImage("testImage");
		dbUser14.setPassword("password");
		dbUser14.setCreateTime(formatter.parse("2015-09-13"));
		dbUser14.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser15=new DBUser();
		dbUser15.setName("testUser");
		dbUser15.setEmail("testEmial");
		dbUser15.setComment("comment");
		dbUser15.setEnabled(true);
		dbUser15.setId(0L);
		dbUser15.setImage("testImage");
		dbUser15.setPassword("password");
		dbUser15.setCreateTime(formatter.parse("2015-09-13"));
		dbUser15.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser16=new DBUser();
		dbUser16.setName("testUser");
		dbUser16.setEmail("testEmial");
		dbUser16.setComment("comment");
		dbUser16.setEnabled(false);
		dbUser16.setId(1L);
		dbUser16.setImage("testImage");
		dbUser16.setPassword("password");
		dbUser16.setCreateTime(formatter.parse("2015-09-13"));
		dbUser16.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser17=new DBUser();
		dbUser17.setName("testUser");
		dbUser17.setEmail("testEmial");
		dbUser17.setComment("comment");
		dbUser17.setEnabled(false);
		dbUser17.setId(0L);
		dbUser17.setImage("testImage17");
		dbUser17.setPassword("password");
		dbUser17.setCreateTime(formatter.parse("2015-09-13"));
		dbUser17.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser18=new DBUser();
		dbUser18.setName("testUser");
		dbUser18.setEmail("testEmial");
		dbUser18.setComment("comment");
		dbUser18.setEnabled(false);
		dbUser18.setId(0L);
		dbUser18.setImage("testImage");
		dbUser18.setPassword("password");
		dbUser18.setCreateTime(formatter.parse("2015-09-14"));
		dbUser18.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser19=new DBUser();
		dbUser19.setName("testUser");
		dbUser19.setEmail("testEmial");
		dbUser19.setComment("comment");
		dbUser19.setEnabled(false);
		dbUser19.setId(0L);
		dbUser19.setImage("testImage");
		dbUser19.setPassword("password");
		dbUser19.setCreateTime(formatter.parse("2015-09-13"));
		dbUser19.setLastUpdateTime(formatter.parse("2015-09-14"));
		
		DBUser dbUser20=new DBUser();
		dbUser20.setName("testUser");
		dbUser20.setEmail("testEmial");
		dbUser20.setComment("comment");
		dbUser20.setEnabled(false);
		dbUser20.setId(0L);
		dbUser20.setImage("testImage");
		dbUser20.setPassword("password20");
		dbUser20.setCreateTime(formatter.parse("2015-09-13"));
		dbUser20.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUser dbUser21=new DBUser();
		dbUser21.setName("testUser");
		dbUser21.setEmail("testEmial");
		dbUser21.setComment("comment");
		dbUser21.setId(0L);
		dbUser21.setImage("testImage");
		dbUser21.setPassword("password20");
		dbUser21.setCreateTime(formatter.parse("2015-09-13"));
		dbUser21.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		assertFalse(dbUser2.equals(dbUser));
		assertFalse(dbUser3.equals(dbUser));
		assertFalse(dbUser4.equals(dbUser));
		assertFalse(dbUser5.equals(dbUser));
		assertFalse(dbUser6.equals(dbUser));
		assertFalse(dbUser7.equals(dbUser));
		assertFalse(dbUser8.equals(dbUser));
		assertFalse(dbUser9.equals(dbUser));
		assertFalse(dbUser10.equals(dbUser));
		assertFalse(dbUser11.equals(dbUser));
		assertFalse(dbUser12.equals(dbUser));
		assertFalse(dbUser13.equals(dbUser));
		assertFalse(dbUser14.equals(dbUser));
		assertFalse(dbUser15.equals(dbUser));
		assertFalse(dbUser16.equals(dbUser));
		assertFalse(dbUser17.equals(dbUser));
		assertFalse(dbUser18.equals(dbUser));
		assertFalse(dbUser19.equals(dbUser));
		assertFalse(dbUser20.equals(dbUser));
		assertFalse(dbUser21.equals(dbUser));
		assertTrue(dbUser1.equals(dbUser));
		assertTrue(dbUser1.hashCode()==dbUser.hashCode());
	}
	
	@Test
	public void testTableModel() throws ParseException{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		DBTable dbTable=new DBTable();
		dbTable.setName("testTabel");
		dbTable.setColumns("column");
		dbTable.setComment("comment");
		dbTable.setId(0L);
		dbTable.setDataSourceName("testDatasource");
		dbTable.setCreateTime(formatter.parse("2015-09-13"));
		dbTable.setLastUpdateTime(formatter.parse("2015-09-15"));
		assertEquals("testTabel",dbTable.getName());
		assertEquals("column",dbTable.getColumns());
		assertEquals("comment",dbTable.getComment());
		assertTrue(0L==dbTable.getId());
		assertEquals("testDatasource",dbTable.getDataSourceName());
		assertEquals(formatter.parse("2015-09-13"),dbTable.getCreateTime());
		assertEquals(formatter.parse("2015-09-15"),dbTable.getLastUpdateTime());

		DBTable dbTable2=new DBTable();
		dbTable2.setName("testTabel");
		dbTable2.setColumns("column");
		dbTable2.setComment("comment");
		dbTable2.setId(0L);
		dbTable2.setDataSourceName("testDatasource");
		dbTable2.setCreateTime(formatter.parse("2015-09-13"));
		dbTable2.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable3=new DBTable();
		dbTable3.setName("testTabel");
		dbTable3.setColumns("column");
		dbTable3.setComment("comment");
		dbTable3.setId(1L);
		dbTable3.setDataSourceName("testDatasource");
		dbTable3.setCreateTime(formatter.parse("2015-09-13"));
		dbTable3.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable4=new DBTable();
		dbTable4.setName("testTabel");
		dbTable4.setComment("comment");
		dbTable4.setId(0L);
		dbTable4.setDataSourceName("testDatasource");
		dbTable4.setCreateTime(formatter.parse("2015-09-13"));
		dbTable4.setLastUpdateTime(formatter.parse("2015-09-15"));

		
		DBTable dbTable5=new DBTable();
		dbTable5.setName("testTabel");
		dbTable5.setColumns("column");
		dbTable5.setId(0L);
		dbTable5.setDataSourceName("testDatasource");
		dbTable5.setCreateTime(formatter.parse("2015-09-13"));
		dbTable5.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable6=new DBTable();
		dbTable6.setName("testTabel");
		dbTable6.setColumns("column");
		dbTable6.setComment("comment");
		dbTable6.setId(0L);
		dbTable6.setCreateTime(formatter.parse("2015-09-13"));
		dbTable6.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable7=new DBTable();
		dbTable7.setName("testTabel");
		dbTable7.setColumns("column");
		dbTable7.setComment("comment");
		dbTable7.setDataSourceName("testDatasource");
		dbTable7.setCreateTime(formatter.parse("2015-09-13"));
		dbTable7.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable8=new DBTable();
		dbTable8.setName("testTabel");
		dbTable8.setColumns("column");
		dbTable8.setComment("comment");
		dbTable8.setId(0L);
		dbTable8.setDataSourceName("testDatasource");
		dbTable8.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable9=new DBTable();
		dbTable9.setName("testTabel");
		dbTable9.setColumns("column");
		dbTable9.setComment("comment");
		dbTable9.setId(0L);
		dbTable9.setDataSourceName("testDatasource");
		dbTable9.setCreateTime(formatter.parse("2015-09-13"));

		
		DBTable dbTable10=new DBTable();
		dbTable10.setName("testTabel10");
		dbTable10.setColumns("column");
		dbTable10.setComment("comment");
		dbTable10.setId(0L);
		dbTable10.setDataSourceName("testDatasource");
		dbTable10.setCreateTime(formatter.parse("2015-09-13"));
		dbTable10.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable11=new DBTable();
		dbTable11.setName("testTabel");
		dbTable11.setColumns("column11");
		dbTable11.setComment("comment");
		dbTable11.setId(0L);
		dbTable11.setDataSourceName("testDatasource");
		dbTable11.setCreateTime(formatter.parse("2015-09-13"));
		dbTable11.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable12=new DBTable();
		dbTable12.setName("testTabel");
		dbTable12.setColumns("column");
		dbTable12.setComment("comment12");
		dbTable12.setId(0L);
		dbTable12.setDataSourceName("testDatasource");
		dbTable12.setCreateTime(formatter.parse("2015-09-13"));
		dbTable12.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable13=new DBTable();
		dbTable13.setName("testTabel");
		dbTable13.setColumns("column");
		dbTable13.setComment("comment");
		dbTable13.setId(0L);
		dbTable13.setDataSourceName("testDatasource13");
		dbTable13.setCreateTime(formatter.parse("2015-09-13"));
		dbTable13.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable14=new DBTable();
		dbTable14.setName("testTabel");
		dbTable14.setColumns("column");
		dbTable14.setComment("comment");
		dbTable14.setId(0L);
		dbTable14.setDataSourceName("testDatasource");
		dbTable14.setCreateTime(formatter.parse("2015-09-14"));
		dbTable14.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBTable dbTable15=new DBTable();
		dbTable15.setName("testTabel");
		dbTable15.setColumns("column");
		dbTable15.setComment("comment");
		dbTable15.setId(0L);
		dbTable15.setDataSourceName("testDatasource");
		dbTable15.setCreateTime(formatter.parse("2015-09-13"));
		dbTable15.setLastUpdateTime(formatter.parse("2015-09-14"));
		
		DBTable dbTable16=new DBTable();
		dbTable16.setName("testTabel");
		dbTable16.setColumns("column");
		dbTable16.setComment("comment");
		dbTable16.setId(0L);
		dbTable16.setDataSourceName("testDatasource6");
		dbTable16.setCreateTime(formatter.parse("2015-09-13"));
		dbTable16.setLastUpdateTime(formatter.parse("2015-09-15"));
		

		assertTrue(dbTable2.equals(dbTable));
		assertFalse(dbTable3.equals(dbTable));
		assertFalse(dbTable4.equals(dbTable));
		assertFalse(dbTable5.equals(dbTable));
		assertFalse(dbTable6.equals(dbTable));
		assertFalse(dbTable7.equals(dbTable));
		assertFalse(dbTable8.equals(dbTable));
		assertFalse(dbTable9.equals(dbTable));
		assertFalse(dbTable10.equals(dbTable));
		assertFalse(dbTable11.equals(dbTable));
		assertFalse(dbTable12.equals(dbTable));
		assertFalse(dbTable13.equals(dbTable));
		assertFalse(dbTable14.equals(dbTable));
		assertFalse(dbTable15.equals(dbTable));
		assertFalse(dbTable16.equals(dbTable));
		assertTrue(dbTable.hashCode()==dbTable2.hashCode());
	}
	
	@Test
	public void testUserGroupModel() throws ParseException{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		DBUserGroup dbUserGroup=new DBUserGroup();
		dbUserGroup.setGroupName("testGroup");
		dbUserGroup.setUserName("testUser");
		dbUserGroup.setId(0L);
		dbUserGroup.setCreateTime(formatter.parse("2015-09-13"));
		dbUserGroup.setLastUpdateTime(formatter.parse("2015-09-15"));
		assertEquals("testUser",dbUserGroup.getUserName());
		assertEquals("testGroup",dbUserGroup.getGroupName());
		assertTrue(0L==dbUserGroup.getId());
		assertEquals(formatter.parse("2015-09-13"),dbUserGroup.getCreateTime());
		assertEquals(formatter.parse("2015-09-15"),dbUserGroup.getLastUpdateTime());

		DBUserGroup dbUserGroup2=new DBUserGroup();
		dbUserGroup2.setGroupName("testGroup");
		dbUserGroup2.setUserName("testUser");
		dbUserGroup2.setId(0L);
		dbUserGroup2.setCreateTime(formatter.parse("2015-09-13"));
		dbUserGroup2.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUserGroup dbUserGroup3=new DBUserGroup();
		dbUserGroup3.setGroupName("testGroup");
		dbUserGroup3.setId(0L);
		dbUserGroup3.setCreateTime(formatter.parse("2015-09-13"));
		dbUserGroup3.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUserGroup dbUserGroup4=new DBUserGroup();
		dbUserGroup4.setUserName("testUser");
		dbUserGroup4.setId(0L);
		dbUserGroup4.setCreateTime(formatter.parse("2015-09-13"));
		dbUserGroup4.setLastUpdateTime(formatter.parse("2015-09-15"));

		DBUserGroup dbUserGroup5=new DBUserGroup();
		dbUserGroup5.setGroupName("testGroup");
		dbUserGroup5.setUserName("testUser");
		dbUserGroup5.setCreateTime(formatter.parse("2015-09-13"));
		dbUserGroup5.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUserGroup dbUserGroup6=new DBUserGroup();
		dbUserGroup6.setGroupName("testGroup");
		dbUserGroup6.setUserName("testUser");
		dbUserGroup6.setId(0L);
		dbUserGroup6.setCreateTime(formatter.parse("2015-09-13"));

		
		DBUserGroup dbUserGroup7=new DBUserGroup();
		dbUserGroup7.setGroupName("testGroup");
		dbUserGroup7.setUserName("testUser");
		dbUserGroup7.setId(0L);
		dbUserGroup7.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUserGroup dbUserGroup8=new DBUserGroup();
		dbUserGroup8.setGroupName("testGroup8");
		dbUserGroup8.setUserName("testUser");
		dbUserGroup8.setId(0L);
		dbUserGroup8.setCreateTime(formatter.parse("2015-09-13"));
		dbUserGroup8.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUserGroup dbUserGroup9=new DBUserGroup();
		dbUserGroup9.setGroupName("testGroup");
		dbUserGroup9.setUserName("testUser9");
		dbUserGroup9.setId(0L);
		dbUserGroup9.setCreateTime(formatter.parse("2015-09-13"));
		dbUserGroup9.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUserGroup dbUserGroup10=new DBUserGroup();
		dbUserGroup10.setGroupName("testGroup");
		dbUserGroup10.setUserName("testUser");
		dbUserGroup10.setId(1L);
		dbUserGroup10.setCreateTime(formatter.parse("2015-09-13"));
		dbUserGroup10.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUserGroup dbUserGroup11=new DBUserGroup();
		dbUserGroup11.setGroupName("testGroup");
		dbUserGroup11.setUserName("testUser");
		dbUserGroup11.setId(0L);
		dbUserGroup11.setCreateTime(formatter.parse("2015-09-14"));
		dbUserGroup11.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBUserGroup dbUserGroup12=new DBUserGroup();
		dbUserGroup12.setGroupName("testGroup");
		dbUserGroup12.setUserName("testUser");
		dbUserGroup12.setId(0L);
		dbUserGroup12.setCreateTime(formatter.parse("2015-09-13"));
		dbUserGroup12.setLastUpdateTime(formatter.parse("2015-09-14"));
		
		assertTrue(dbUserGroup2.equals(dbUserGroup));
		assertFalse(dbUserGroup3.equals(dbUserGroup));
		assertFalse(dbUserGroup4.equals(dbUserGroup));
		assertFalse(dbUserGroup5.equals(dbUserGroup));
		assertFalse(dbUserGroup6.equals(dbUserGroup));
		assertFalse(dbUserGroup7.equals(dbUserGroup));
		assertFalse(dbUserGroup8.equals(dbUserGroup));
		assertFalse(dbUserGroup9.equals(dbUserGroup));
		assertFalse(dbUserGroup10.equals(dbUserGroup));
		assertFalse(dbUserGroup11.equals(dbUserGroup));
		assertFalse(dbUserGroup12.equals(dbUserGroup));
		assertTrue(dbUserGroup.hashCode()==dbUserGroup2.hashCode());
	}
	@Test
	public void testRightGroupModel() throws ParseException{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		DBRightGroup dbRightGroup=new DBRightGroup();
		dbRightGroup.setGroupName("testGroup");
		dbRightGroup.setRightName("testRight");
		dbRightGroup.setRightType(0);
		dbRightGroup.setId(0L);
		dbRightGroup.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup.setLastUpdateTime(formatter.parse("2015-09-15"));
		assertEquals("testGroup",dbRightGroup.getGroupName());
		assertEquals("testRight",dbRightGroup.getRightName());
		assertTrue(0==dbRightGroup.getRightType());
		assertTrue(0L==dbRightGroup.getId());
		assertEquals(formatter.parse("2015-09-13"),dbRightGroup.getCreateTime());
		assertEquals(formatter.parse("2015-09-15"),dbRightGroup.getLastUpdateTime());

		DBRightGroup dbRightGroup2=new DBRightGroup();
		dbRightGroup2.setGroupName("testGroup");
		dbRightGroup2.setRightName("testRight");
		dbRightGroup2.setRightType(0);
		dbRightGroup2.setId(0L);
		dbRightGroup2.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup2.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBRightGroup dbRightGroup3=new DBRightGroup();
		dbRightGroup3.setGroupName("testGroup3");
		dbRightGroup3.setRightName("testRight3");
		dbRightGroup3.setRightType(1);
		dbRightGroup3.setId(1L);
		dbRightGroup3.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup3.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBRightGroup dbRightGroup4=new DBRightGroup();
		dbRightGroup4.setGroupName("testGroup");
		dbRightGroup4.setRightType(0);
		dbRightGroup4.setId(0L);
		dbRightGroup4.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup4.setLastUpdateTime(formatter.parse("2015-09-15"));

		
		DBRightGroup dbRightGroup5=new DBRightGroup();
		dbRightGroup5.setGroupName("testGroup");
		dbRightGroup5.setRightName("testRight");
		dbRightGroup5.setId(0L);
		dbRightGroup5.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup5.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBRightGroup dbRightGroup6=new DBRightGroup();
		dbRightGroup6.setGroupName("testGroup");
		dbRightGroup6.setRightName("testRight");
		dbRightGroup6.setRightType(0);
		dbRightGroup6.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup6.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBRightGroup dbRightGroup7=new DBRightGroup();
		dbRightGroup7.setGroupName("testGroup");
		dbRightGroup7.setRightName("testRight");
		dbRightGroup7.setRightType(0);
		dbRightGroup7.setId(0L);
		dbRightGroup7.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBRightGroup dbRightGroup8=new DBRightGroup();
		dbRightGroup8.setGroupName("testGroup");
		dbRightGroup8.setRightName("testRight");
		dbRightGroup8.setRightType(0);
		dbRightGroup8.setId(0L);
		dbRightGroup8.setCreateTime(formatter.parse("2015-09-13"));
		
		DBRightGroup dbRightGroup9=new DBRightGroup();
		dbRightGroup9.setGroupName("testGroup9");
		dbRightGroup9.setRightName("testRight");
		dbRightGroup9.setRightType(0);
		dbRightGroup9.setId(0L);
		dbRightGroup9.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup9.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBRightGroup dbRightGroup10=new DBRightGroup();
		dbRightGroup10.setGroupName("testGroup");
		dbRightGroup10.setRightName("testRight10");
		dbRightGroup10.setRightType(0);
		dbRightGroup10.setId(0L);
		dbRightGroup10.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup10.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBRightGroup dbRightGroup11=new DBRightGroup();
		dbRightGroup11.setGroupName("testGroup");
		dbRightGroup11.setRightName("testRight");
		dbRightGroup11.setRightType(0);
		dbRightGroup11.setId(1L);
		dbRightGroup11.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup11.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBRightGroup dbRightGroup12=new DBRightGroup();
		dbRightGroup12.setGroupName("testGroup");
		dbRightGroup12.setRightName("testRight");
		dbRightGroup12.setRightType(0);
		dbRightGroup12.setId(0L);
		dbRightGroup12.setCreateTime(formatter.parse("2015-09-14"));
		dbRightGroup12.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		
		DBRightGroup dbRightGroup13=new DBRightGroup();
		dbRightGroup13.setGroupName("testGroup");
		dbRightGroup13.setRightName("testRight");
		dbRightGroup13.setRightType(1);
		dbRightGroup13.setId(0L);
		dbRightGroup13.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup13.setLastUpdateTime(formatter.parse("2015-09-15"));
		
		DBRightGroup dbRightGroup14=new DBRightGroup();
		dbRightGroup14.setGroupName("testGroup");
		dbRightGroup14.setRightName("testRight");
		dbRightGroup14.setRightType(0);
		dbRightGroup14.setId(0L);
		dbRightGroup14.setCreateTime(formatter.parse("2015-09-13"));
		dbRightGroup14.setLastUpdateTime(formatter.parse("2015-09-14"));
		
		assertTrue(dbRightGroup2.equals(dbRightGroup));
		assertFalse(dbRightGroup3.equals(dbRightGroup));
		assertFalse(dbRightGroup4.equals(dbRightGroup));
		assertFalse(dbRightGroup5.equals(dbRightGroup));
		assertFalse(dbRightGroup6.equals(dbRightGroup));
		assertFalse(dbRightGroup7.equals(dbRightGroup));
		assertFalse(dbRightGroup8.equals(dbRightGroup));
		assertFalse(dbRightGroup9.equals(dbRightGroup));
		assertFalse(dbRightGroup10.equals(dbRightGroup));
		assertFalse(dbRightGroup11.equals(dbRightGroup));
		assertFalse(dbRightGroup12.equals(dbRightGroup));
		assertFalse(dbRightGroup13.equals(dbRightGroup));
		assertFalse(dbRightGroup14.equals(dbRightGroup));
		assertTrue(dbRightGroup.hashCode()==dbRightGroup2.hashCode());
	}
}
