/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import java.util.Set;

import com.google.common.collect.Sets;

/**
 * 
 * @author mingmwang
 *
 */
public class TableDimension {
	private String name;
	protected Set<String> columnNames = Sets.newHashSet();
	private int type;
	private boolean multiValue = false;
	private boolean required = false;
	
	public TableDimension() {
	}
	
	public TableDimension(String name, int type) {
		this.name = name;
		this.type = type;
		columnNames.add(name);
	}
	
	public String getName() {
		return name;
	}
	
	public Set<String> getColumnNames() {
		return columnNames;
	}

	public void setName(String name) {
		this.name = name;
		columnNames.add(name);
	}
	
	public int getType() {
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}
	
	public boolean isMultiValue() {
		return multiValue;
	}

	public void setMultiValue(boolean multiValue) {
		this.multiValue = multiValue;
	}
	
	
	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}

	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((columnNames == null) ? 0 : columnNames.hashCode());
		result = prime * result + (multiValue ? 1231 : 1237);
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (required ? 1231 : 1237);
		result = prime * result + type;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TableDimension other = (TableDimension) obj;
		if (columnNames == null) {
			if (other.columnNames != null)
				return false;
		} else if (!columnNames.equals(other.columnNames))
			return false;
		if (multiValue != other.multiValue)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (required != other.required)
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TableDimension [name=" + name + ", columnNames=" + columnNames
				+ ", type=" + type + ", multiValue=" + multiValue + ",  required=" + required + "]";
	}
	
}
