/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarDataSourceConfiguration extends DataSourceConfiguration{
	private String rtolapName;
	private String molapName;
	private Collection<PulsarTable> pulsarTableConfiguration;
	private Set<String> excludeTableSets;
	private Map<String, PulsarDataSourceRule> routConfiguration;
	
	public PulsarDataSourceConfiguration(String dataSourceName, String rtolapName){
		super(DataSourceTypeEnum.PULSAR, dataSourceName);
		super.setRealOnly(true);
		this.rtolapName = rtolapName;
	}
	
	public PulsarDataSourceConfiguration(String dataSourceName, String rtolapName, String molapName){
		super(DataSourceTypeEnum.PULSAR, dataSourceName);
		super.setRealOnly(true);
		this.rtolapName = rtolapName;
		this.molapName = molapName;
	}
	
	public String getRtolapName() {
		return rtolapName;
	}

	public String getMolapName() {
		return molapName;
	}

	public Set<String> getExcludeTableSets() {
		return excludeTableSets;
	}

	public void setExcludeTableSets(Set<String> excludeTableSets) {
		this.excludeTableSets = excludeTableSets;
	}

	public Collection<PulsarTable> getPulsarTableConfiguration() {
		return pulsarTableConfiguration;
	}

	public void setPulsarTableConfiguration(
			Collection<PulsarTable> pulsarTableConfiguration) {
		this.pulsarTableConfiguration = pulsarTableConfiguration;
	}

	public Map<String, PulsarDataSourceRule> getRoutConfiguration() {
		return routConfiguration;
	}

	public void setRoutConfiguration(
			Map<String, PulsarDataSourceRule> routConfiguration) {
		this.routConfiguration = routConfiguration;
	}
}
