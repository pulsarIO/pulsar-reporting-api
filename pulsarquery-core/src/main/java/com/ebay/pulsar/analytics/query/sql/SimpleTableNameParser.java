/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.sql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;

/**
 * FundationDb didn't support "." "." structure in the SQL Grammer, so use regex to parse table name from SQL.
 * Only support one table name now.
 * @param sql
 * @return tableName
 * 
 * @author mingmwang
 */
public class SimpleTableNameParser {
	
	private static String SQL_PATTERN_STR = "select (.*) from\\s+([^ ,]+)(?:\\s*,\\s*([^ ,]+))*\\s+";
	private static Pattern SQL_PATTERN = Pattern.compile(SQL_PATTERN_STR, Pattern.CASE_INSENSITIVE);
	
	public static String getTableName(String sql){
		if(Strings.isNullOrEmpty(sql)){
			return null;
		}
		Matcher matcher = SQL_PATTERN.matcher(sql);
		if(matcher.find()){
			return matcher.group(2);
		}else{
			return null;
		}
	}
}
