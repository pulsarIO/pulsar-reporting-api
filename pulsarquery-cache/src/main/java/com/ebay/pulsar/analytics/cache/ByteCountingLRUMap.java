/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.cache;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ByteCountingLRUMap extends LinkedHashMap<ByteBuffer, byte[]> {
	private static final long serialVersionUID = -7099965804364170905L;

	private final boolean logEvictions;
	private final int logEvictionCount;
	private final long sizeInBytes;

	private volatile long numBytes;
	// Sonar complained about volatile long increment
	private AtomicLong evictionCount;

	public ByteCountingLRUMap(final long sizeInBytes) {
		this(16, 0, sizeInBytes);
	}

	public ByteCountingLRUMap(final int initialSize, final int logEvictionCount, final long sizeInBytes) {
		super(initialSize, 0.75f, true);
		this.logEvictionCount = logEvictionCount;
		this.sizeInBytes = sizeInBytes;

		logEvictions = logEvictionCount != 0;
		numBytes = 0;
		evictionCount = new AtomicLong(0);
	}

	public long getNumBytes() {
		return numBytes;
	}

	public long getEvictionCount() {
		return evictionCount.get();
	}

	@Override
	public byte[] put(ByteBuffer key, byte[] value) {
		numBytes += key.remaining() + value.length;
		return super.put(key, value);
	}

	@Override
	public boolean removeEldestEntry(Map.Entry<ByteBuffer, byte[]> eldest) {
		if (numBytes > sizeInBytes) {
			// ++evictionCount;
			evictionCount.incrementAndGet();
			if (logEvictions && evictionCount.get() % logEvictionCount == 0) {
				// log.info("Evicting %,dth element.  Size[%,d], numBytes[%,d], averageSize[%,d]",
				// evictionCount, size(), numBytes, numBytes / size());
			}

			numBytes -= eldest.getKey().remaining() + eldest.getValue().length;
			return true;
		}
		return false;
	}

	@Override
	public byte[] remove(Object key) {
		byte[] value = super.remove(key);
		if (value != null) {
			numBytes -= ((ByteBuffer) key).remaining() + value.length;
		}
		return value;
	}

	/**
	 * Don't allow key removal using the underlying keySet iterator All removal
	 * operations must use ByteCountingLRUMap.remove()
	 */
	@Override
	public Set<ByteBuffer> keySet() {
		return Collections.unmodifiableSet(super.keySet());
	}

	@Override
	public void clear() {
		numBytes = 0;
		super.clear();
	}
}
