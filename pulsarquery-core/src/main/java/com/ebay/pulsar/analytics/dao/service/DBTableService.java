/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao.service;

import org.springframework.jdbc.core.RowMapper;

import com.ebay.pulsar.analytics.dao.mapper.DBTableMapper;
import com.ebay.pulsar.analytics.dao.model.DBTable;



public class DBTableService extends BaseDBService<DBTable> {
	@Override
	public String getTableName() {
		//return Configs.getString("pulsareye.monitor.db.table.checker","PULSAREYE_CHECKER");
		return getTablePrefix()+"DBTables";
	}

	@Override
	public RowMapper<DBTable> mapper() {
		return new DBTableMapper();
	}

}
