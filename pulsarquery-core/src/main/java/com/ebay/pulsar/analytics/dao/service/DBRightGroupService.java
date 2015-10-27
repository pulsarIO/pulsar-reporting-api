/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao.service;

import org.springframework.jdbc.core.RowMapper;

import com.ebay.pulsar.analytics.dao.mapper.DBRightGroupMapper;
import com.ebay.pulsar.analytics.dao.model.DBRightGroup;
import com.google.common.collect.ImmutableMap;



public class DBRightGroupService extends BaseDBService<DBRightGroup> {
	@Override
	public String getTableName() {
		//return Configs.getString("pulsareye.monitor.db.table.checker","PULSAREYE_CHECKER");
		return getTablePrefix()+"DBRightGroup";
	}

	@Override
	public RowMapper<DBRightGroup> mapper() {
		return new DBRightGroupMapper();
	}
	public int deleteRightsFromGroupByPrefix(String rightNamePrefix){
		String prefix=rightNamePrefix.endsWith("%")?rightNamePrefix:rightNamePrefix+"%";
		return execute("delete from "+QUTOA+getTableName()+QUTOA+ "  where rightName like :rightName"
				,ImmutableMap.of("rightName",prefix));
	}
}
