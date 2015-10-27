/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.cache;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

public class MapCache implements Cache {
	public static Cache create(long sizeInBytes) {
		return new MapCache(new ByteCountingLRUMap(sizeInBytes));
	}

	private final Map<ByteBuffer, byte[]> baseMap;
	private final ByteCountingLRUMap byteCountingLRUMap;

	private final Map<String, byte[]> namespaceId;
	private final AtomicInteger ids;

	private final Object clearLock = new Object();

	private final AtomicLong getBytes = new AtomicLong(0);
	private final AtomicLong hitCount = new AtomicLong(0);
	private final AtomicLong missCount = new AtomicLong(0);

	private AtomicLong cacheGetTime  = new AtomicLong(0);

	private MapCache(ByteCountingLRUMap byteCountingLRUMap) {
		this.byteCountingLRUMap = byteCountingLRUMap;
		this.baseMap = Collections.synchronizedMap(byteCountingLRUMap);

		namespaceId = Maps.newHashMap();
		ids = new AtomicInteger();
	}

	@Override
	public CacheStats getStats() {
		return new CacheStats(getBytes.get(), cacheGetTime.get(), byteCountingLRUMap.size(),
				byteCountingLRUMap.getNumBytes(), hitCount.get(), missCount.get(),
				byteCountingLRUMap.getEvictionCount(), 0, 0);
	}

	@Override
	public byte[] get(NamedKey key) {
		final byte[] retVal;
		long start = System.nanoTime();
		synchronized (clearLock) {
			retVal = baseMap.get(computeKey(getNamespaceId(key.namespace), key.key));
		}
		if (retVal == null) {
			missCount.incrementAndGet();
		} else {
			getBytes.addAndGet(retVal.length);
			hitCount.incrementAndGet();
		}
		cacheGetTime.addAndGet(System.nanoTime() - start);
		return retVal;
	}

	@Override
	public void put(NamedKey key, byte[] value, int expiration) {
		synchronized (clearLock) {
			baseMap.put(computeKey(getNamespaceId(key.namespace), key.key), value);
		}
	}

	@Override
	public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys) {
		Map<NamedKey, byte[]> retVal = Maps.newHashMap();
		for (NamedKey key : keys) {
			retVal.put(key, get(key));
		}
		return retVal;
	}

	@Override
	public void close(String namespace) {
		byte[] idBytes;
		synchronized (namespaceId) {
			idBytes = getNamespaceId(namespace);
			if (idBytes == null) {
				return;
			}
			namespaceId.remove(namespace);
		}
		synchronized (clearLock) {
			Iterator<ByteBuffer> iter = baseMap.keySet().iterator();
			List<ByteBuffer> toRemove = Lists.newLinkedList();
			while (iter.hasNext()) {
				ByteBuffer next = iter.next();
				if (next.get(0) == idBytes[0] && next.get(1) == idBytes[1] && next.get(2) == idBytes[2]
						&& next.get(3) == idBytes[3]) {
					toRemove.add(next);
				}
			}
			for (ByteBuffer key : toRemove) {
				baseMap.remove(key);
			}
		}
	}

	private byte[] getNamespaceId(final String identifier) {
		synchronized (namespaceId) {
			byte[] idBytes = namespaceId.get(identifier);
			if (idBytes != null) {
				return idBytes;
			}
			idBytes = Ints.toByteArray(ids.getAndIncrement());
			namespaceId.put(identifier, idBytes);
			return idBytes;
		}
	}

	private ByteBuffer computeKey(byte[] idBytes, byte[] key) {
		final ByteBuffer retVal = ByteBuffer.allocate(key.length + 4).put(idBytes).put(key);
		retVal.rewind();
		return retVal;
	}

	@Override
	public boolean isLocal() {
		return true;
	}
}