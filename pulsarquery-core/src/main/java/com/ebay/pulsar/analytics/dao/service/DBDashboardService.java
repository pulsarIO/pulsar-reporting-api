/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao.service;

import org.springframework.jdbc.core.RowMapper;

import com.ebay.pulsar.analytics.dao.mapper.DBDashboardMapper;
import com.ebay.pulsar.analytics.dao.model.DBDashboard;


public class DBDashboardService extends BaseDBService<DBDashboard> {
	
	@Override
	public String getTableName() {
		return getTablePrefix()+"DBDashboard";
	}

	@Override
	public RowMapper<DBDashboard> mapper() {
		return new DBDashboardMapper();
	}

}
