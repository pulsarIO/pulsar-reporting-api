/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao.service;

import org.springframework.jdbc.core.RowMapper;

import com.ebay.pulsar.analytics.dao.mapper.DBGroupMapper;
import com.ebay.pulsar.analytics.dao.model.DBGroup;



public class DBGroupService extends BaseDBService<DBGroup> {
	@Override
	public String getTableName() {
		//return Configs.getString("pulsareye.monitor.db.table.checker","PULSAREYE_CHECKER");
		return getTablePrefix()+"DBGroup";
	}

	@Override
	public RowMapper<DBGroup> mapper() {
		return new DBGroupMapper();
	}

}
