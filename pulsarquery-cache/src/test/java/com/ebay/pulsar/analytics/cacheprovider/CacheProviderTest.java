/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.cacheprovider;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.ebay.pulsar.analytics.cache.CacheConfig;
import com.ebay.pulsar.analytics.cache.CacheProvider;
import com.ebay.pulsar.analytics.cache.MemcachedCacheConfig;

public class CacheProviderTest {

	@Test
	public void testProvider() {
		// CacheConfig test
		CacheConfig cc = new CacheConfig();
		cc.setExpirations(null);
		cc.setPopulateCache(false);
		cc.setUnCacheable(null);
		cc.setUseCache(false);
		try {
			cc.getExpiration("");
		} catch (Exception e) {

		}
		Assert.assertTrue(cc.getExpirations() == null);
		Assert.assertTrue(cc.isUseCache() == false);
		Assert.assertTrue(cc.isPopulateCache() == false);
		try {
			cc.isQueryCacheable("tttt");
		} catch (Exception e) {

		}

//		ArrayList<String> al = new ArrayList<String>();
//		al.add("test");
		cc.setUnCacheable("test");
		Assert.assertTrue(!cc.isQueryCacheable("test"));

		HashMap<String, Integer> mp = new HashMap<String, Integer>();
		mp.put("test2", 1000);
		cc.setExpirations(mp);
		Assert.assertTrue(cc.getExpiration("test2") == 1000);

		// MemcachedCacheConfig test
		MemcachedCacheConfig mc = new MemcachedCacheConfig();
		mc.setMaxObjectSize(mc.getMaxObjectSize());
		mc.setMaxOperationQueueSize(mc.getMaxOperationQueueSize());
		mc.setMemcachedPrefix(mc.getMemcachedPrefix());
		mc.setReadBufferSize(mc.getReadBufferSize());
		mc.setTimeout(mc.getMaxObjectSize());
		Assert.assertTrue(mc.getHosts() == null);

		// provider test
		CacheProvider p = new CacheProvider();
		p.setCacheConfig(cc);
		p.setMemConfig(mc);

		Assert.assertTrue(p.getCacheConfig().equals(cc));
		Assert.assertTrue(p.getMemConfig().equals(mc));

		p.setType("ratio");
		Assert.assertTrue(p.getType().equals("ratio"));
		Assert.assertTrue(p.get() == null);

		p.setType("memcached");
		//ArrayList<String> al2 = new ArrayList<String>();
		//al2.add("localhost:8080");
		mc.setHosts("localhost:8080");

		//Cache m = null;

/*		Cache c = Mockito.mock(Cache.class);
		
		MemcachedCache.create(mc)
		try {
			m = p.get();
		} catch (Exception e) {

		}*/
		
/*		if (m != null)
			Assert.assertTrue(m instanceof MemcachedCache);*/

	}
}
