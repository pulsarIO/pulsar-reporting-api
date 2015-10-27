/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.cache;

public class CacheStats {
	private final long numHits;
	private final long numMisses;
	private final long getBytes;
	private final long getTime;
	private final long numPut;
	private final long putBytes;
	private final long numEvictions;
	private final long numTimeouts;
	private final long numErrors;

	public CacheStats(long getBytes, long getTime, long numPut, long putBytes, long numHits, long numMisses,
			long numEvictions, long numTimeouts, long numErrors) {
		this.getBytes = getBytes;
		this.getTime = getTime;
		this.numPut = numPut;
		this.putBytes = putBytes;
		this.numHits = numHits;
		this.numMisses = numMisses;
		this.numEvictions = numEvictions;
		this.numTimeouts = numTimeouts;
		this.numErrors = numErrors;
	}

	public long getNumHits() {
		return numHits;
	}

	public long getNumMisses() {
		return numMisses;
	}

	public long getNumGet() {
		return numHits + numMisses;
	}

	public long getNumGetBytes() {
		return getBytes;
	}

	public long getNumPutBytes() {
		return putBytes;
	}

	public long getNumPut() {
		return numPut;
	}

	public long getNumEvictions() {
		return numEvictions;
	}

	public long getNumTimeouts() {
		return numTimeouts;
	}

	public long getNumErrors() {
		return numErrors;
	}

	public long numLookups() {
		return numHits + numMisses;
	}

	public double hitRate() {
		long lookups = numLookups();
		return lookups == 0 ? 0 : numHits / (double) lookups;
	}

	public long avgGetBytes() {
		return getBytes == 0 ? 0 : getBytes / numLookups();
	}

	public long getAvgGetTime() {
		return getTime / numLookups();
	}
}
