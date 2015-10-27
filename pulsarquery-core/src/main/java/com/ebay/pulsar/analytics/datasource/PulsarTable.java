/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarTable extends Table {
	
	public PulsarTable() {
	}
	
	public PulsarTable(Table table) {
		super.setTableName(table.getTableName());
		super.setDimensions(table.getDimensions());
		super.setMetrics(table.getMetrics());
	}
	
	private String tableNameAlias;
	
	private String rtolapTableName;
	private String molapTableName;
	
	public String getRTOLAPTableName() {
		return rtolapTableName;
	}

	public void setRTOLAPTableName(String rtolapTableName) {
		this.rtolapTableName = rtolapTableName;
		super.tableNames.add(rtolapTableName);
	}

	public String getMOLAPTableName() {
		return molapTableName;
	}

	public void setMOLAPTableName(String molapTableName) {
		this.molapTableName = molapTableName;
		super.tableNames.add(molapTableName);
	}
			
	public String getTableNameAlias() {
		return tableNameAlias;
	}

	public void setTableNameAlias(String tableNameAlias) {
		this.tableNameAlias = tableNameAlias;
		super.tableNames.addAll(Sets.newHashSet(Splitter.on(',').trimResults().split(tableNameAlias)));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((rtolapTableName == null) ? 0 : rtolapTableName.hashCode());
		result = prime * result
				+ ((molapTableName == null) ? 0 : molapTableName.hashCode());
		result = prime * result
				+ ((tableNameAlias == null) ? 0 : tableNameAlias.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		PulsarTable other = (PulsarTable) obj;
		if (rtolapTableName == null) {
			if (other.rtolapTableName != null)
				return false;
		} else if (!rtolapTableName.equals(other.rtolapTableName))
			return false;
		if (molapTableName == null) {
			if (other.molapTableName != null)
				return false;
		} else if (!molapTableName.equals(other.molapTableName))
			return false;
		if (tableNameAlias == null) {
			if (other.tableNameAlias != null)
				return false;
		} else if (!tableNameAlias.equals(other.tableNameAlias))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PulsarTable [tableNameAlias=" + tableNameAlias
				+ ", rtolapTableName=" + rtolapTableName + ", molapTableName="
				+ molapTableName + ", superTable= " + super.toString() + " ]";
	}
}
