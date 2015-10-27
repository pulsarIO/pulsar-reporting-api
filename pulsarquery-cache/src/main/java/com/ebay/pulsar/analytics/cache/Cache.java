/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

public interface Cache {
	byte[] get(NamedKey key);

	void put(NamedKey key, byte[] value, int expiration);

	Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys);

	void close(String namespace);

	CacheStats getStats();

	boolean isLocal();
	
	public class NamedKey {
		final public String namespace;
		final public byte[] key;

		public NamedKey(String namespace, byte[] key) {
			Preconditions.checkArgument(namespace != null, "namespace must not be null");
			Preconditions.checkArgument(key != null, "key must not be null");
			this.namespace = namespace;
			this.key = key;
		}

		public byte[] toByteArray() {
			final byte[] nsBytes = this.namespace.getBytes(Charsets.UTF_8);
			return ByteBuffer.allocate(Ints.BYTES + nsBytes.length + this.key.length).putInt(nsBytes.length)
					.put(nsBytes).put(this.key).array();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			NamedKey namedKey = (NamedKey) o;

			if (!namespace.equals(namedKey.namespace)) {
				return false;
			}
			if (!Arrays.equals(key, namedKey.key)) {
				return false;
			}
			return true;
		}

		@Override
		public int hashCode() {
			return 31 * namespace.hashCode() + Arrays.hashCode(key);
		}
	}
}