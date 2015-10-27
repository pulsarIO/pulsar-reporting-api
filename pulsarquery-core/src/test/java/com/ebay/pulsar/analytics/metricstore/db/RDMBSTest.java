/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.metricstore.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.ebay.pulsar.analytics.dao.DBFactory;
import com.ebay.pulsar.analytics.dao.RDBMS;


public class RDMBSTest {
	@SuppressWarnings("unchecked")
	@Test
	public void testRDBMS() {
		String driver2 = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://locahost:3306/test";
		String userName = "root";
		String userPwd = "";
		BasicDataSource bds = new BasicDataSource();
		bds.setDriverClassName(driver2);
		bds.setUrl(url);
		bds.setUsername(userName);
		bds.setPassword(userPwd);
		DBFactory.setDs(bds);
		List<String> result = new ArrayList<String>();
		result.add("result");
		NamedParameterJdbcTemplate namedParameterJdbcTemplate = Mockito
				.mock(NamedParameterJdbcTemplate.class);
		when(
				namedParameterJdbcTemplate.query(Matchers.anyString(),
						Matchers.any(Map.class),
						Matchers.any(ResultSetExtractor.class))).thenReturn(
				result);
		RDBMS db = new RDBMS(driver2, url, userName,
				userPwd);
		db.setDriver(driver2);
		db.setNamedParameterJdbcTemplate(namedParameterJdbcTemplate);
		db.setUrl(url);
		db.setUserName(userName);
		db.setUserPwd(userPwd);
		assertEquals(namedParameterJdbcTemplate,
				db.getNamedParameterJdbcTemplate());
		Map<String, String> map = new HashMap<String, String>();
		assertTrue(result.equals(db.queryForList("test", map, 10)));

	}

	@Test
	public void testRDBMS2() {
		String driver2 = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://locahost:3306/test";
		String userName = "root";
		String userPwd = "";
		BasicDataSource bds = new BasicDataSource();
		bds.setDriverClassName(driver2);
		bds.setUrl(url);
		bds.setUsername(userName);
		bds.setPassword(userPwd);
		DBFactory.setDs(bds);
		List<String> result = new ArrayList<String>();
		result.add("result");
		NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(
				bds);
		RDBMS db = new RDBMS(bds);
		db.setDriver(driver2);
		db.setNamedParameterJdbcTemplate(namedParameterJdbcTemplate);
		db.setUrl(url);
		db.setUserName(userName);
		db.setUserPwd(userPwd);
		assertEquals(userName, db.getUserName());
		assertEquals(url, db.getUrl());
		assertEquals(userPwd, db.getUserPwd());

	}

}
