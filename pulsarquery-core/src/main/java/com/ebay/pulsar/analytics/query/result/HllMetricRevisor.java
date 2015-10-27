/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.result;

import java.util.Set;

/**
 * 
 * @author mingmwang
 *
 */
public class HllMetricRevisor implements ResultRevisor {
	public Set<String> hllSet;
	
	public HllMetricRevisor(Set<String> hllSet) {
		this.hllSet = hllSet;
	}

	@Override
	public void revise(ResultNode node) {
		if (hllSet != null && hllSet.contains(node.getName())) {
			Long hllLongValue = ((Number) node.getValue()).longValue();
			node.setValue(hllLongValue);
		}
	}
}
