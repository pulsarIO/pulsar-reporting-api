/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao.service;

import org.springframework.jdbc.core.RowMapper;

import com.ebay.pulsar.analytics.dao.mapper.DBDataSourceMapper;
import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.service.BaseDBService;



public class DBDataSourceService extends BaseDBService<DBDataSource> {

	@Override
	public String getTableName() {
		return getTablePrefix()+"DBDatasource";
	}

	@Override
	public RowMapper<DBDataSource> mapper() {
		return new DBDataSourceMapper();
	}

}
