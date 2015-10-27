/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao;

import java.util.List;
import java.util.Map;

/**
 * Database Operation.
 * 
 *@author qxing
 * 
 **/
public interface DBService<T> {
	
	public T getById(long id);
	public List<T> getAll();
	public List<T> get(T condition);
	public int updateById(T update);
	public long inser(T insert);
	public int deleteById(long id);
	public int deleteBatch(T condition);
	public int execute(String sql,Map<String,?> param);
}
