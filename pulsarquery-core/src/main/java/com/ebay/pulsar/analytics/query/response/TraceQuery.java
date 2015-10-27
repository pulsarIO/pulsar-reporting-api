/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.response;

import java.util.Arrays;

/**
 * 
 * @author rtao
 *
 */
public class TraceQuery {
	private boolean fromcache;
	private boolean tocache;
	private int bytesize;
	private long cachegettime;
	private long druidquerytime = 0;
	private byte[] cachekey;
	private Object query;

	public boolean isFromcache() {
		return fromcache;
	}

	public void setFromcache(boolean fromcache) {
		this.fromcache = fromcache;
	}

	public boolean isTocache() {
		return tocache;
	}

	public void setTocache(boolean tocache) {
		this.tocache = tocache;
	}

	public int getBytesize() {
		return bytesize;
	}

	public void setBytesize(int bytesize) {
		this.bytesize = bytesize;
	}

	public long getCachegettime() {
		return cachegettime;
	}

	public void setCachegettime(long cachegettime) {
		this.cachegettime = cachegettime;
	}

	public long getDruidquerytime() {
		return druidquerytime;
	}

	public void setDruidquerytime(long druidquerytime) {
		this.druidquerytime += druidquerytime;
	}

	public byte[] getCachekey() {
		return cachekey;
	}

	public void setCachekey(byte[] cachekey) {
		this.cachekey = cachekey;
	}

	public Object getQuery() {
		return query;
	}

	public void setQuery(Object query) {
		this.query = query;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + bytesize;
		result = prime * result + (int) (cachegettime ^ (cachegettime >>> 32));
		result = prime * result + Arrays.hashCode(cachekey);
		result = prime * result
				+ (int) (druidquerytime ^ (druidquerytime >>> 32));
		result = prime * result + (fromcache ? 1231 : 1237);
		result = prime * result + ((query == null) ? 0 : query.hashCode());
		result = prime * result + (tocache ? 1231 : 1237);
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
		TraceQuery other = (TraceQuery) obj;
		if (bytesize != other.bytesize)
			return false;
		if (cachegettime != other.cachegettime)
			return false;
		if (!Arrays.equals(cachekey, other.cachekey))
			return false;
		if (druidquerytime != other.druidquerytime)
			return false;
		if (fromcache != other.fromcache)
			return false;
		if (query == null) {
			if (other.query != null)
				return false;
		} else if (!query.equals(other.query))
			return false;
		if (tocache != other.tocache)
			return false;
		return true;
	}
	
	
}
