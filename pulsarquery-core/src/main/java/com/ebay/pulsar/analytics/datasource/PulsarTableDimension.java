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
public class PulsarTableDimension extends TableDimension {
	private String alias;

	public PulsarTableDimension(){
	}
	
	
	public PulsarTableDimension(String name, int type) {
		super(name, type);
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
		super.columnNames.addAll(Sets.newHashSet(Splitter.on(',').trimResults().split(alias)));
	}
		
	private String molapColumnName;
	private String rtolapColumnName;

	public String getMOLAPColumnName() {
		return molapColumnName;
	}

	public void setMOLAPColumnName(String kylinColumnName) {
		this.molapColumnName = kylinColumnName;
		super.columnNames.add(kylinColumnName);
	}
	
	public String getRTOLAPColumnName() {
		return rtolapColumnName;
	}

	public void setRTOLAPColumnName(String druidColumnName) {
		this.rtolapColumnName = druidColumnName;
		super.columnNames.add(druidColumnName);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((alias == null) ? 0 : alias.hashCode());
		result = prime * result
				+ ((rtolapColumnName == null) ? 0 : rtolapColumnName.hashCode());
		result = prime * result
				+ ((molapColumnName == null) ? 0 : molapColumnName.hashCode());
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
		PulsarTableDimension other = (PulsarTableDimension) obj;
		if (alias == null) {
			if (other.alias != null)
				return false;
		} else if (!alias.equals(other.alias))
			return false;
		if (rtolapColumnName == null) {
			if (other.rtolapColumnName != null)
				return false;
		} else if (!rtolapColumnName.equals(other.rtolapColumnName))
			return false;
		if (molapColumnName == null) {
			if (other.molapColumnName != null)
				return false;
		} else if (!molapColumnName.equals(other.molapColumnName))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "PulsarTableDimension [alias=" + alias + ", molapColumnName="
				+ molapColumnName + ", rtolapColumnName=" + rtolapColumnName
				+ ", superDimensions = " + super.toString() + " ]";
	}
}
