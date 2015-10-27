/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao.service;

import org.springframework.jdbc.core.RowMapper;

import com.ebay.pulsar.analytics.dao.mapper.DBUserGroupMapper;
import com.ebay.pulsar.analytics.dao.model.DBUserGroup;



public class DBUserGroupService extends BaseDBService<DBUserGroup> {
	@Override
	public String getTableName() {
		//return Configs.getString("pulsareye.monitor.db.table.checker","PULSAREYE_CHECKER");
		return getTablePrefix()+"DBUserGroup";
	}

	@Override
	public RowMapper<DBUserGroup> mapper() {
		return new DBUserGroupMapper();
	}

}
