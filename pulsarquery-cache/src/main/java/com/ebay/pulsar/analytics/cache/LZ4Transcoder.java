/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.cache;

import java.nio.ByteBuffer;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4Factory;
import net.spy.memcached.transcoders.SerializingTranscoder;

import com.google.common.primitives.Ints;

public class LZ4Transcoder extends SerializingTranscoder {
	private final LZ4Factory lz4Factory;

	public LZ4Transcoder() {
		super();
		lz4Factory = LZ4Factory.fastestJavaInstance();
	}

	public LZ4Transcoder(int max) {
		super(max);
		lz4Factory = LZ4Factory.fastestJavaInstance();
	}

	@Override
	protected byte[] compress(byte[] in) {
		if (in == null) {
			throw new NullPointerException("Can't compress null");
		}

		LZ4Compressor compressor = lz4Factory.fastCompressor();

		byte[] out = new byte[compressor.maxCompressedLength(in.length)];
		int compressedLength = compressor.compress(in, 0, in.length, out, 0);

		getLogger().debug("Compressed %d bytes to %d", in.length, compressedLength);

		return ByteBuffer.allocate(Ints.BYTES + compressedLength).putInt(in.length).put(out, 0, compressedLength).array();
	}

	@Override
	protected byte[] decompress(byte[] in) {
		byte[] out = null;
		if (in != null) {
			LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();

			int size = ByteBuffer.wrap(in).getInt();
			out = new byte[size];
			decompressor.decompress(in, Ints.BYTES, out, 0, out.length);
		}
		return out == null ? null : out;
	}
}
