/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.cache;

import java.util.List;

import com.google.common.base.Splitter;

import net.spy.memcached.DefaultConnectionFactory;

public class MemcachedCacheConfig {
	private int timeout = 500;

	// comma delimited list of memcached servers, given as host:port combination
	private List<String> hosts;

	private int maxObjectSize = 50 * 1024 * 1024;

	// memcached client read buffer size, -1 uses the spymemcached library default
	private int readBufferSize = DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE;

	private String memcachedPrefix = "druid-api";

	// maximum size in bytes of memcached client operation queue. 0 means unbounded
	private long maxOperationQueueSize = 0;
	
	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public List<String> getHosts() {
		
		return this.hosts;
	}

	public void setHosts(String hosts) {
		this.hosts=Splitter.on(",").omitEmptyStrings().splitToList(hosts);
		System.out.println("hosts="+hosts);
	}

	public int getMaxObjectSize() {
		return maxObjectSize;
	}

	public void setMaxObjectSize(int maxObjectSize) {
		this.maxObjectSize = maxObjectSize;
	}

	public String getMemcachedPrefix() {
		return memcachedPrefix;
	}

	public void setMemcachedPrefix(String memcachedPrefix) {
		this.memcachedPrefix = memcachedPrefix;
	}

	public long getMaxOperationQueueSize() {
		return maxOperationQueueSize;
	}

	public void setMaxOperationQueueSize(long maxOperationQueueSize) {
		this.maxOperationQueueSize = maxOperationQueueSize;
	}

	public int getReadBufferSize() {
		return readBufferSize;
	}

	public void setReadBufferSize(int readBufferSize) {
		this.readBufferSize = readBufferSize;
	}

}
