/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;

public class CacheConfig {
	private boolean useCache;
	private boolean populateCache;
	private List<String> unCacheable;
	private Map<String, Integer> expirations;

	public void setUseCache(boolean useCache) {
		this.useCache = useCache;
	}

	public void setPopulateCache(boolean populateCache) {
		this.populateCache = populateCache;
	}

	public void setUnCacheable(String unCacheable) {
		if(unCacheable == null)
			this.unCacheable =  new ArrayList<String>();
		else
			this.unCacheable = Splitter.on(",").omitEmptyStrings().splitToList(unCacheable);
	}

	public boolean isPopulateCache() {
		return populateCache;
	}

	public boolean isUseCache() {
		return useCache;
	}

	public boolean isQueryCacheable(String queryTypeName) {
		if(unCacheable!=null)
			return !unCacheable.contains(queryTypeName);
		else
			throw new NullPointerException("unCacheable is null");
	}

	public int getExpiration(String type) {
		if(expirations!=null)
			return expirations.get(type);
		else
			throw new NullPointerException("expirations is null");
	}

	public Map<String, Integer> getExpirations() {
		return expirations;
	}

	public void setExpirations(Map<String, Integer> expirations) {
		this.expirations = expirations;
	}
}
