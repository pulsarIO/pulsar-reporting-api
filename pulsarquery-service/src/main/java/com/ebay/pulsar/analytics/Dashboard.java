/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics;

import java.util.Date;
import java.util.Map;

import com.ebay.pulsar.analytics.dao.model.DBDashboard;
import com.ebay.pulsar.analytics.util.JsonUtil;

/**
 *@author qxing
 * 
 **/
public class Dashboard {
	private Long id;
	private String name;
	private String displayName;
	private String owner;
	private Map<String, Object> config;
	private Date createTime;
	private Date lastUpdateTime;
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDisplayName() {
		return displayName;
	}
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
	public Date getLastUpdateTime() {
		return lastUpdateTime;
	}
	public void setLastUpdateTime(Date lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
	}
	public Map<String, Object> getConfig() {
		return config;
	}
	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((config == null) ? 0 : config.hashCode());
		result = prime * result
				+ ((createTime == null) ? 0 : createTime.hashCode());
		result = prime * result
				+ ((displayName == null) ? 0 : displayName.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result
				+ ((lastUpdateTime == null) ? 0 : lastUpdateTime.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((owner == null) ? 0 : owner.hashCode());
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
		Dashboard other = (Dashboard) obj;
		if (config == null) {
			if (other.config != null)
				return false;
		} else if (!config.equals(other.config))
			return false;
		if (createTime == null) {
			if (other.createTime != null)
				return false;
		} else if (!createTime.equals(other.createTime))
			return false;
		if (displayName == null) {
			if (other.displayName != null)
				return false;
		} else if (!displayName.equals(other.displayName))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (lastUpdateTime == null) {
			if (other.lastUpdateTime != null)
				return false;
		} else if (!lastUpdateTime.equals(other.lastUpdateTime))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (owner == null) {
			if (other.owner != null)
				return false;
		} else if (!owner.equals(other.owner))
			return false;
		return true;
	}
	
	@SuppressWarnings("unchecked")
	public static Dashboard from(DBDashboard dd){
		Dashboard d=new Dashboard();
		try{
		d.setConfig(JsonUtil.readValue(dd.getConfig(), Map.class));
		}catch(Exception e){
			d.setConfig(null);
		}
		d.setCreateTime(dd.getCreateTime());
		d.setDisplayName(dd.getDisplayName());
		d.setId(dd.getId());
		d.setLastUpdateTime(dd.getLastUpdateTime());
		d.setName(dd.getName());
		d.setOwner(dd.getOwner());
		return d;
	}
	
	public DBDashboard toDBDashboard(){
		DBDashboard d=new DBDashboard();
		try{
			d.setConfig(JsonUtil.writeValueAsIndentString(config));
		}catch(Exception e){
			d.setConfig(null);
		}
		d.setCreateTime(getCreateTime());
		d.setDisplayName(getDisplayName());
		d.setName(getName());
		d.setOwner(getOwner());
		return d;
	}
}
