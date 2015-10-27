/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.memcachedcache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ops.Operation;

import org.junit.Test;
import org.junit.Assert;

import com.ebay.pulsar.analytics.cache.BytesBoundedLinkedQueue;
import com.ebay.pulsar.analytics.cache.MemcachedOperationQueueFactory;

public class BytesBoundedLinkedQueueTest {

	public class StringFactory {
		public long maxQueueSize = 1000;

		public void MemcachedOperationQueueFactory(long maxQueueSize) {
			this.maxQueueSize = maxQueueSize;
		}

		public BlockingQueue<String> create() {
			return new BytesBoundedLinkedQueue<String>(maxQueueSize) {
				@Override
				public long getBytesSize(String operation) {
					return operation.getBytes().length;
				}
			};
		}
	}

	@Test
	public void testBytesBoundedLinkedQueue() {
		MemcachedOperationQueueFactory fac = new MemcachedOperationQueueFactory(
				1000);
		BytesBoundedLinkedQueue<Operation> queue = (BytesBoundedLinkedQueue<Operation>) fac
				.create();
		
		queue.isEmpty();

		StringFactory sf = new StringFactory();
		BytesBoundedLinkedQueue<String> sq = (BytesBoundedLinkedQueue<String>) sf
				.create();
		sq.offer("a");

		try {
			Assert.assertTrue(sq.peek().equals("a"));
			Assert.assertTrue(sq.take().equals("a"));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			sq.offer(null);
		} catch (NullPointerException e) {
		}

		//Assert.assertTrue(sq.size() == 0);

		try {
			Assert.assertTrue(sq.offer("b", 10, TimeUnit.NANOSECONDS));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		ArrayList<String> al = new ArrayList<String>();
		sq.drainTo(al);
		//Assert.assertTrue(al.size() == 1);

		try {
			sq.drainTo(null);
		} catch (Exception e) {
		}
		try {
			sq.drainTo(sq);
		} catch (Exception e) {
		}

		sq.offer("c");
		Assert.assertTrue(sq.poll().equals("c"));
		
		sq.offer("d");
		try {
			Assert.assertTrue(sq.poll(100, TimeUnit.NANOSECONDS).equals("d"));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		sq.offer("e");
		sq.offer("f");
		
		
		Iterator<String> itr= sq.iterator();
		boolean first = true;
		while(itr.hasNext()){
			String t = itr.next();
			if(first){
				itr.remove();
				first = false;
			}
			else{
				Assert.assertTrue(t.equals("f"));
			}
		}
		
		sq.poll();
		
		Assert.assertTrue(sq.remainingCapacity()==1000);
	}
}
