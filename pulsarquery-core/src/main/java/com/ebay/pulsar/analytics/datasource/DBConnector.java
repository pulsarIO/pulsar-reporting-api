/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import java.util.Set;

/**
 * 
 * @author mingmwang
 *
 */
public interface DBConnector extends Starter, ShutDown {
	public Object query(Object query);
	public Set<String> getAllTables();
	public Table getTableMeta(String tableName);
}
