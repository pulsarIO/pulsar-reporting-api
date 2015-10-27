/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.aggregator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.AggregatorType;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

/**
 * 
 * @author rtao
 *
 */
public class CardinalityAggregator extends BaseAggregator {
	private List<String> fieldNames = new ArrayList<String>();
	private boolean byRow = false;

	private static final byte CACHE_TYPE_ID = (byte) 0x8;

	public CardinalityAggregator(String name, List<String> fieldNames) {
		super(AggregatorType.cardinality, name);
		this.fieldNames = fieldNames;
	}

	public List<String> getFieldNames() {
		return fieldNames;
	}

	public boolean getByRow() {
		return byRow;
	}

	public void setByRow(boolean byRow) {
		this.byRow = byRow;
	}

	@Override
	public byte[] cacheKey() {
		byte[] nameBytes = super.getName().getBytes();
		byte[] fieldNameBytes = Joiner.on("\u0001").join(fieldNames).getBytes(Charsets.UTF_8);

		return ByteBuffer.allocate(1 + nameBytes.length + fieldNameBytes.length).put(CACHE_TYPE_ID)
				.put(nameBytes).put(fieldNameBytes).array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (byRow ? 1231 : 1237);
		result = prime * result
				+ ((fieldNames == null) ? 0 : fieldNames.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		CardinalityAggregator other = (CardinalityAggregator) obj;
		if (byRow != other.byRow)
			return false;
		if (fieldNames == null) {
			if (other.fieldNames != null)
				return false;
		} else if (!fieldNames.equals(other.fieldNames))
			return false;
		return true;
	}
	
	
}
