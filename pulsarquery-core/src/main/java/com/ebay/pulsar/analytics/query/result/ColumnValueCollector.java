/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.result;

import java.util.Collection;

import com.google.common.collect.Sets;

/**
 * 
 * @author mingmwang
 *
 * @param <V>
 */
public class ColumnValueCollector<V> implements ResultRevisor {
	private String columnName;
	private Collection<V> valueCollection;
	
	public ColumnValueCollector(String columnName) {
		this(columnName, Sets.<V>newHashSet());
	}

	public ColumnValueCollector(String columnName, Collection<V> valueCollection) {
		super();
		this.columnName = columnName;
		this.valueCollection = valueCollection;
	}


	@SuppressWarnings("unchecked")
	@Override
	public void revise(ResultNode node) {
		if(columnName.equals(node.getName())){
			valueCollection.add((V)node.getValue());
		}
	}

	public Collection<V> getValueCollection() {
		return valueCollection;
	}

	public String getColumnName() {
		return columnName;
	}
}
