/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.cache;

public class CacheProvider {
	// cache type, memcached or others
	private String type;

	private CacheConfig cacheConfig;

	private MemcachedCacheConfig memConfig;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public CacheConfig getCacheConfig() {
		return cacheConfig;
	}

	public void setCacheConfig(CacheConfig cacheConfig) {
		this.cacheConfig = cacheConfig;
	}

	public MemcachedCacheConfig getMemConfig() {
		return memConfig;
	}

	public void setMemConfig(MemcachedCacheConfig memConfig) {
		this.memConfig = memConfig;
	}

	public Cache get() {
		if (type.equals("memcached")) {
			return MemcachedCache.create(memConfig);
		} else {
			return null; // other cache types
		}
	}
}
