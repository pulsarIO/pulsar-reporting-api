/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.mapcache;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.ebay.pulsar.analytics.cache.ByteCountingLRUMap;
import com.ebay.pulsar.analytics.cache.Cache;
import com.ebay.pulsar.analytics.cache.Cache.NamedKey;
import com.ebay.pulsar.analytics.cache.CacheStats;
import com.ebay.pulsar.analytics.cache.LZ4Transcoder;
import com.ebay.pulsar.analytics.cache.MapCache;

public class MapCacheTest {
	
	public class LZ4TranscoderEx extends LZ4Transcoder {
		public byte[] compress(byte[] in) {
			return super.compress(in);
		}
		
		public byte[] decompress(byte[] in) {
			return super.decompress(in);
		}
	}
	
	@Test
	public void TestLZ4Transcoder(){
		LZ4TranscoderEx l = new LZ4TranscoderEx();
		
		byte [] a = l.compress("test".getBytes());
		
		String st = new String(l.decompress(a));
		Assert.assertTrue(st.equals("test"));
	}
	
	@Test
	public void testNameKey(){
		NamedKey nk = new NamedKey("space", "key".getBytes());
		
		Assert.assertTrue(nk.equals(nk));
		Assert.assertTrue(nk.equals(null)==false);
		NamedKey nk2 = new NamedKey("space1", "key".getBytes());
		Assert.assertTrue(nk2.equals(null)==false);
		NamedKey nk3 = new NamedKey("space", "key1".getBytes());
		Assert.assertTrue(nk3.equals(null)==false);
		NamedKey nk4 = new NamedKey("space", "key".getBytes());
		Assert.assertTrue(nk4.equals(null)==false);
	}
	
	@Test
	public void testMapCache(){
		Cache c =  MapCache.create(10000);
		NamedKey nk = new NamedKey("space", "key".getBytes());
		
		c.put(nk, "result".getBytes(), 10000);
		String result = new String(c.get(nk));
		Assert.assertTrue(result.equals("result"));
	
		NamedKey nk2 = new NamedKey("space2", "key2".getBytes());
		Assert.assertTrue(c.get(nk2)==null);
		
		ArrayList<NamedKey> al = new ArrayList<NamedKey>(); 
		al.add(nk);
		al.add(nk2);
		Assert.assertTrue(c.getBulk(al).size()==2);
		Assert.assertTrue(new String(c.getBulk(al).get(nk)).equals("result"));
		Assert.assertTrue(c.getBulk(al).get(nk2)==null);
		
		CacheStats cs = c.getStats();
		Assert.assertTrue(cs.getNumTimeouts()==0);
		Assert.assertTrue(cs.getNumErrors()==0);
		c.close("space");	
	}
	
	@Test
	public void testByteCountingLRUMap (){
		ByteBuffer key =  ByteBuffer.allocate(1000);
		key.put("test".getBytes());
		
		ByteCountingLRUMap lm = new ByteCountingLRUMap (10000);
		lm.put(key, "result".getBytes());		
		
		byte [] rb = lm.get(key);
		String re = null;
		try{
			 re = new String(rb);
		}catch(Exception e){
			
		}
		Assert.assertTrue(re.equals("result"));
		lm.clear();
		
		ByteCountingLRUMap lm2 = new ByteCountingLRUMap (1000);
		lm2.put(key, "result".getBytes());
		byte [] rb2 = lm.get(key);
		Assert.assertTrue(rb2==null);
		
		/*Map<ByteBuffer, byte[]> tm = new HashMap<ByteBuffer, byte[]>();
		tm.put(key, "reslt".getBytes());
		Map.Entry<ByteBuffer, byte[]> eldest = tm.entrySet().iterator().next();
		
		Assert.assertTrue(lm.removeEldestEntry(eldest));*/
		
	}

}
