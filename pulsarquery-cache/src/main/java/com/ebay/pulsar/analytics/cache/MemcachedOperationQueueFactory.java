/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.cache;

import java.util.concurrent.BlockingQueue;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationQueueFactory;

public class MemcachedOperationQueueFactory implements OperationQueueFactory {
	public final long maxQueueSize;

	public MemcachedOperationQueueFactory(long maxQueueSize) {
		this.maxQueueSize = maxQueueSize;
	}

	@Override
	public BlockingQueue<Operation> create() {
		return new BytesBoundedLinkedQueue<Operation>(maxQueueSize) {
			@Override
			public long getBytesSize(Operation operation) {
				return operation.getBuffer().remaining();
			}
		};
	}
}