/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.result;

import java.util.List;
import java.util.Map;


/**
 * 
 * @author mingmwang
 *
 */
public class ColumnNameRevisor implements ResultRevisor {
	private List<String> dimensions;
	private Map<String, String> nameAliasMap;
	
	public ColumnNameRevisor(List<String> dimensions,
			Map<String, String> nameAliasMap) {
		this.dimensions = dimensions;
		this.nameAliasMap = nameAliasMap;
	}

	@Override
	public void revise(ResultNode node) {
		if(dimensions!=null && dimensions.contains(node.getName())){
			 String newColumnName = replaceColumnToUserRequest(node.getName(), nameAliasMap);
			 node.setName(newColumnName);
		}
	}
	
	private String replaceColumnToUserRequest(String columnName, Map<String, String> nameAliasMap) {
		if(nameAliasMap != null && nameAliasMap.get(columnName) != null){
			return nameAliasMap.get(columnName);
		}
		return columnName;
	}
}
