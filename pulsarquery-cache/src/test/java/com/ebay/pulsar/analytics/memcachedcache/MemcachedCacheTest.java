/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.memcachedcache;

import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.internal.BulkFuture;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.ebay.pulsar.analytics.cache.Cache.NamedKey;
import com.ebay.pulsar.analytics.cache.CacheStats;
import com.ebay.pulsar.analytics.cache.MemcachedCache;
import com.ebay.pulsar.analytics.cache.MemcachedCacheConfig;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

public class MemcachedCacheTest {

	private static String computeKeyHash(String memcachedPrefix, NamedKey key) {
		// hash keys to keep things under 250 characters for memcached
		return memcachedPrefix + ":" + DigestUtils.shaHex(key.namespace) + ":"
				+ DigestUtils.shaHex(key.key);
	}

	private static byte[] serializeValue(NamedKey key, byte[] value) {
		byte[] keyBytes = key.toByteArray();
		return ByteBuffer.allocate(Ints.BYTES + keyBytes.length + value.length)
				.putInt(keyBytes.length).put(keyBytes).put(value).array();
	}

	@Mock
	Future<Boolean> future1;
	@Mock
	Future<Object> future2;
	@Mock
	Future<Object> future3;
	@Mock
	Future<Object> future4;
	@Mock
	Future<Object> future5;
	@Mock
	BulkFuture<Map<String, Object>> future6;
	@Mock
	BulkFuture<Map<String, Object>> future7;
	@Mock
	BulkFuture<Map<String, Object>> future8;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void MemcachedCache() {
		// MemcachedCacheConfig test
		final MemcachedCacheConfig mc = new MemcachedCacheConfig();
		mc.setMaxObjectSize(mc.getMaxObjectSize());
		mc.setMaxOperationQueueSize(mc.getMaxOperationQueueSize());
		mc.setMemcachedPrefix(mc.getMemcachedPrefix());
		mc.setReadBufferSize(mc.getReadBufferSize());
		mc.setTimeout(mc.getMaxObjectSize());
		Assert.assertTrue(mc.getHosts() == null);
		//ArrayList<String> al2 = new ArrayList<String>();
		//al2.add("localhost:8080");
		mc.setHosts("localhost:8080");

		MemcachedCache INSTANCE = MemcachedCache.create(mc);

		Assert.assertTrue(INSTANCE != null);

		MemcachedClientIF client = Mockito.mock(MemcachedClientIF.class);
		try {
			ReflectFieldUtil.setField(INSTANCE, "client", client);
		} catch (Exception e) {
			e.printStackTrace();
		}

		//regular test
		final NamedKey nk = new NamedKey("space", "key".getBytes());

		when(
				client.set(computeKeyHash(mc.getMemcachedPrefix(), nk), 1000,
						serializeValue(nk, "result".getBytes()))
			)
		.thenAnswer(new Answer<Future<Boolean>>() {
			public Future<Boolean> answer(InvocationOnMock invocation)
					throws Throwable {
				//Future<Boolean> future = Mockito.mock(FutureTask.class);
				when(future1.get()).thenReturn(true);
				return future1;
			}
		});

		try {
			INSTANCE.put(nk, "result".getBytes(), 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		when(
				client.asyncGet(computeKeyHash(mc.getMemcachedPrefix(), nk))
			)
		.thenAnswer(new Answer<Future<Object>>() {
			public Future<Object> answer(InvocationOnMock invocation)
					throws Throwable {
				//Future<Object> future = Mockito.mock(FutureTask.class);
				when(future2.get(mc.getTimeout(),TimeUnit.MILLISECONDS)).thenReturn(serializeValue(nk,"result".getBytes()));
				return future2;
			}
		});
		
		String re = new String(INSTANCE.get(nk));
		Assert.assertTrue(re.equals("result"));
		
		/////client exception
		final NamedKey nkIllegalStateException = new NamedKey("space", "IllegalStateException".getBytes());
		
		when(
				client.asyncGet(computeKeyHash(mc.getMemcachedPrefix(), nkIllegalStateException))
			).thenThrow(new IllegalStateException("nkIllegalStateException") );
		
		Assert.assertTrue(INSTANCE.get(nkIllegalStateException)==null);
		
	
		//future exception
		final NamedKey nkTimeoutException  = new NamedKey("space", "TimeoutException".getBytes());
		when(
				client.asyncGet(computeKeyHash(mc.getMemcachedPrefix(), nkTimeoutException))
			)
		.thenAnswer(new Answer<Future<Object>>() {
			public Future<Object> answer(InvocationOnMock invocation)
					throws Throwable {
				//Future<Object> future = Mockito.mock(FutureTask.class);
				when(future3.get(mc.getTimeout(),TimeUnit.MILLISECONDS)).thenThrow(new TimeoutException("TimeoutException"));
				return future3;
			}
		});
		Assert.assertTrue(INSTANCE.get(nkTimeoutException)==null);
		
		
		final NamedKey nkInterruptedException   = new NamedKey("space", "InterruptedException".getBytes());
		when(
				client.asyncGet(computeKeyHash(mc.getMemcachedPrefix(), nkInterruptedException))
			)
		.thenAnswer(new Answer<Future<Object>>() {
			public Future<Object> answer(InvocationOnMock invocation)
					throws Throwable {
				//Future<Object> future = Mockito.mock(FutureTask.class);
				when(future4.get(mc.getTimeout(),TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException ("InterruptedException"));
				return future4;
			}
		});
		try{
			Assert.assertTrue(INSTANCE.get(nkInterruptedException)==null);	
		}catch(Exception e){
			
		}
		
		
		final NamedKey nkExecutionException  = new NamedKey("space", "ExecutionException".getBytes());
		when(
				client.asyncGet(computeKeyHash(mc.getMemcachedPrefix(), nkExecutionException))
			)
		.thenAnswer(new Answer<Future<Object>>() {
			public Future<Object> answer(InvocationOnMock invocation)
					throws Throwable {
				//Future<Object> future = Mockito.mock(FutureTask.class);
				when(future5.get(mc.getTimeout(),TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("ExecutionException",new Exception("ExecutionException")));
				return future5;
			}
		});
		Assert.assertTrue(INSTANCE.get(nkExecutionException)==null);
		
		////////test bulk
		
		//get bulk fail
		final NamedKey nkIllegalStateExceptionBulk = new NamedKey("space","IllegalStateException".getBytes());
		ArrayList<NamedKey> a1 = new ArrayList<NamedKey>();
		a1.add(nkIllegalStateExceptionBulk);
		Map<String, NamedKey> keyLookup = Maps.uniqueIndex(a1,
				new Function<NamedKey, String>() {
					@Override
					public String apply(NamedKey input) {
						return computeKeyHash(mc.getMemcachedPrefix(), input);
					}
				});
		 
		when(
				client.asyncGetBulk(keyLookup.keySet())
			).thenThrow(new IllegalStateException("nkIllegalStateException") );
		
		Assert.assertTrue(INSTANCE.getBulk(a1).size()==0);
		
		//test future
		final NamedKey some = new NamedKey("space","resultsome".getBytes());
		ArrayList<NamedKey> a2 = new ArrayList<NamedKey>();
		a2.add(some);
		Map<String, NamedKey> keyLookup2 = Maps.uniqueIndex(a2,
				new Function<NamedKey, String>() {
					@Override
					public String apply(NamedKey input) {
						return computeKeyHash(mc.getMemcachedPrefix(), input);
					}
				});
		 
		when(
				client.asyncGetBulk(keyLookup2.keySet())
			).thenAnswer(new Answer<BulkFuture<Map<String, Object>>>() {
				public BulkFuture<Map<String, Object>> answer(InvocationOnMock invocation)
						throws Throwable {
					//BulkFuture<Map<String, Object>> future = Mockito.mock(BulkFuture.class);
					Map<String, Object> mp = new HashMap<String, Object>(); 
					mp.put(computeKeyHash(mc.getMemcachedPrefix(),some), serializeValue(some, "result".getBytes()));
					when(future8.getSome(mc.getTimeout(),TimeUnit.MILLISECONDS)).thenReturn(mp);
					return future8;
				}
			});
		
		String somere = new String(INSTANCE.getBulk(a2).get(some));
		Assert.assertTrue(somere.equals("result"));
		
		
		//test bulk exception
		final NamedKey someInterruptedException = new NamedKey("space","someInterruptedException".getBytes());
		ArrayList<NamedKey> a3 = new ArrayList<NamedKey>();
		a3.add(someInterruptedException);
		Map<String, NamedKey> keyLookup3 = Maps.uniqueIndex(a3,
				new Function<NamedKey, String>() {
					@Override
					public String apply(NamedKey input) {
						return computeKeyHash(mc.getMemcachedPrefix(), input);
					}
				});
		 
		when(
				client.asyncGetBulk(keyLookup3.keySet())
			).thenAnswer(new Answer<BulkFuture<Map<String, Object>>>() {
				public BulkFuture<Map<String, Object>> answer(InvocationOnMock invocation)
						throws Throwable {
					//BulkFuture<Map<String, Object>> future = Mockito.mock(BulkFuture.class);
					when(future6.getSome(mc.getTimeout(),TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException("someInterruptedException"));
					return future6;
				}
			});
		try{
			INSTANCE.getBulk(a3).get(someInterruptedException);
		}catch(Exception e){
			System.out.println("Catch InterruptedException success!");
		}
		
		final NamedKey someExecutionException  = new NamedKey("space","someExecutionException".getBytes());
		ArrayList<NamedKey> a4 = new ArrayList<NamedKey>();
		a4.add(someExecutionException);
		Map<String, NamedKey> keyLookup4 = Maps.uniqueIndex(a4,
				new Function<NamedKey, String>() {
					@Override
					public String apply(NamedKey input) {
						return computeKeyHash(mc.getMemcachedPrefix(), input);
					}
				});
		 
		when(
				client.asyncGetBulk(keyLookup4.keySet())
			).thenAnswer(new Answer<BulkFuture<Map<String, Object>>>() {
				public BulkFuture<Map<String, Object>> answer(InvocationOnMock invocation)
						throws Throwable {
					//BulkFuture<Map<String, Object>> future = Mockito.mock(BulkFuture.class);
					when(future7.getSome(mc.getTimeout(),TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("someExecutionException",new Exception("someExecutionException")));
					return future7;
				}
			});
			
		Assert.assertTrue(INSTANCE.getBulk(a4).get(someExecutionException)==null);
		
		CacheStats st = INSTANCE.getStats();
		Assert.assertTrue(st.getNumErrors()==4);
		Assert.assertTrue(st.getNumHits()==2);
		Assert.assertTrue(st.getNumMisses()==0);
	}
}
