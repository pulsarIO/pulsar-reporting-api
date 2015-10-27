/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.ops.LinkedOperationQueueFactory;
import net.spy.memcached.ops.OperationQueueFactory;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

public class MemcachedCache implements Cache {
	private static final Logger logger = LoggerFactory.getLogger(MemcachedCache.class);
	
	private static volatile MemcachedCache INSTANCE;

	public static MemcachedCache create(final MemcachedCacheConfig config) {
		if (INSTANCE == null) {
			try {
				LZ4Transcoder transcoder = new LZ4Transcoder(config.getMaxObjectSize());

				// always use compression
				transcoder.setCompressionThreshold(0);

				OperationQueueFactory opQueueFactory;
				long maxQueueBytes = config.getMaxOperationQueueSize();
				if (maxQueueBytes > 0) {
					opQueueFactory = new MemcachedOperationQueueFactory(maxQueueBytes);
				} else {
					opQueueFactory = new LinkedOperationQueueFactory();
				}
				String hosts2Str = config.getHosts().toString();
				String hostsList = hosts2Str.substring(1, hosts2Str.length() - 1);

				synchronized (MemcachedCache.class) {
					if (INSTANCE == null) {
						INSTANCE = new MemcachedCache(new MemcachedClient(new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY).setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH).setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT).setDaemon(true).setFailureMode(FailureMode.Cancel).setTranscoder(transcoder).setShouldOptimize(true).setOpQueueMaxBlockTime(config.getTimeout()).setOpTimeout(config.getTimeout()).setReadBufferSize(config.getReadBufferSize())
								.setOpQueueFactory(opQueueFactory).build(), AddrUtil.getAddresses(hostsList)), config);
					}
				}
			} catch (IOException e) {
				logger.error("Unable to create MemcachedCache instance: " + e.getMessage());
				throw Throwables.propagate(e);
			}
		}
		return INSTANCE;
	}

	private final int timeout;
	private final String memcachedPrefix;

	private final MemcachedClientIF client;

	private final AtomicLong getBytes = new AtomicLong(0);
	private final AtomicLong hitCount = new AtomicLong(0);
	private final AtomicLong missCount = new AtomicLong(0);
	private final AtomicLong timeoutCount = new AtomicLong(0);
	private final AtomicLong errorCount = new AtomicLong(0);
	private final AtomicLong putCount = new AtomicLong(0);
	private final AtomicLong putBytes = new AtomicLong(0);
	
	private AtomicLong cacheGetTime = new AtomicLong(0);

	private MemcachedCache(MemcachedClientIF client, MemcachedCacheConfig config) {
		Preconditions.checkArgument(config.getMemcachedPrefix().length() <= MAX_PREFIX_LENGTH, "memcachedPrefix length [%d] exceeds maximum length [%d]", config.getMemcachedPrefix().length(), MAX_PREFIX_LENGTH);
		this.timeout = config.getTimeout();
		this.client = client;
		this.memcachedPrefix = config.getMemcachedPrefix();
	}

	@Override
	public CacheStats getStats() {
		return new CacheStats(getBytes.get(), cacheGetTime.get(), putCount.get(), putBytes.get(), hitCount.get(),
				missCount.get(), 0, timeoutCount.get(), errorCount.get());
	}

	@Override
	public byte[] get(NamedKey key) {
		Future<Object> future;
		long start = System.nanoTime();
		try {
			future = client.asyncGet(computeKeyHash(memcachedPrefix, key));
		} catch (IllegalStateException e) {
			// operation did not get queued in time (queue is full)
			errorCount.incrementAndGet();
			logger.warn("Unable to queue cache operation: " + e.getMessage());
			return null;
		}
		try {
			byte[] bytes = (byte[]) future.get(timeout, TimeUnit.MILLISECONDS);
			cacheGetTime.addAndGet(System.nanoTime() - start);
			if (bytes != null) {
				getBytes.addAndGet(bytes.length);
				hitCount.incrementAndGet();
			} else {
				missCount.incrementAndGet();
			}
			return bytes == null ? null : deserializeValue(key, bytes);
		} catch (TimeoutException e) {
			timeoutCount.incrementAndGet();
			future.cancel(false);
			return null;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw Throwables.propagate(e);
		} catch (ExecutionException e) {
			errorCount.incrementAndGet();
			logger.warn("Exception pulling item from cache: " + e.getMessage());
			return null;
		}
	}

	@Override
	public void put(NamedKey key, byte[] value, int expiration) {
		try {
			putBytes.addAndGet(value.length);
			putCount.incrementAndGet();
			client.set(computeKeyHash(memcachedPrefix, key), expiration, serializeValue(key, value));
		} catch (IllegalStateException e) {
			// operation did not get queued in time (queue is full)
			errorCount.incrementAndGet();
			logger.warn("Unable to queue cache operation: " + e.getMessage());
		}
	}

	private static byte[] serializeValue(NamedKey key, byte[] value) {
		byte[] keyBytes = key.toByteArray();
		return ByteBuffer.allocate(Ints.BYTES + keyBytes.length + value.length).putInt(keyBytes.length).put(keyBytes).put(value).array();
	}

	private static byte[] deserializeValue(NamedKey key, byte[] bytes) {
		ByteBuffer buf = ByteBuffer.wrap(bytes);

		final int keyLength = buf.getInt();
		byte[] keyBytes = new byte[keyLength];
		buf.get(keyBytes);
		byte[] value = new byte[buf.remaining()];
		buf.get(value);

		Preconditions.checkState(Arrays.equals(keyBytes, key.toByteArray()), "Keys do not match, possible hash collision?");
		return value;
	}

	@Override
	public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys) {
		Map<String, NamedKey> keyLookup = Maps.uniqueIndex(keys, new Function<NamedKey, String>() {
			@Override
			public String apply(NamedKey input) {
				return computeKeyHash(memcachedPrefix, input);
			}
		});

		Map<NamedKey, byte[]> results = Maps.newHashMap();

		BulkFuture<Map<String, Object>> future;
		try {
			future = client.asyncGetBulk(keyLookup.keySet());
		} catch (IllegalStateException e) {
			// operation did not get queued in time (queue is full)
			errorCount.incrementAndGet();
			logger.warn("Unable to queue cache operation: " + e.getMessage());
			return results;
		}

		try {
			Map<String, Object> some = future.getSome(timeout, TimeUnit.MILLISECONDS);

			if (future.isTimeout()) {
				future.cancel(false);
				timeoutCount.incrementAndGet();
			}
			missCount.addAndGet(keyLookup.size() - some.size());
			hitCount.addAndGet(some.size());

			for (Map.Entry<String, Object> entry : some.entrySet()) {
				final NamedKey key = keyLookup.get(entry.getKey());
				final byte[] value = (byte[]) entry.getValue();
				results.put(key, value == null ? null : deserializeValue(key, value));
			}
			return results;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw Throwables.propagate(e);
		} catch (ExecutionException e) {
			errorCount.incrementAndGet();
			logger.warn("Exception pulling item from cache: " + e.getMessage());
			return results;
		}
	}

	@Override
	public void close(String namespace) {
		// no resources to cleanup
	}

	public static final int MAX_PREFIX_LENGTH =
			MemcachedClientIF.MAX_KEY_LENGTH
			- 40 // length of namespace hash
			- 40 // length of key hash
			- 2;  // length of separators

	private static String computeKeyHash(String memcachedPrefix, NamedKey key) {
		// hash keys to keep things under 250 characters for memcached
		return memcachedPrefix + ":" + DigestUtils.shaHex(key.namespace) + ":" + DigestUtils.shaHex(key.key);
	}

	@Override
	public boolean isLocal() {
		return false;
	}
}
