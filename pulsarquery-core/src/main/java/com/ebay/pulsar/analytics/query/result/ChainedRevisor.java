/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.result;

import java.util.List;

import com.google.common.collect.Lists;


/**
 * 
 * @author mingmwang
 *
 */
public class ChainedRevisor implements ResultRevisor {
	
	List<? extends ResultRevisor> revisorList;
	
	public ChainedRevisor(ResultRevisor ...revisor) {
		this.revisorList = Lists.newArrayList(revisor);
	}
	
	public ChainedRevisor(List<? extends ResultRevisor> revisorList) {
		this.revisorList = revisorList;
	}
	@Override
	public void revise(ResultNode node) {
		for(ResultRevisor revisor : revisorList){
			revisor.revise(node);
		}
	}
}
