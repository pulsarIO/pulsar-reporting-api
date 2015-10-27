/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.client;

/**
 * 
 * @author mingmwang
 *
 */
public class ClientQueryConfig {

	private int connectTimeout = 10000;
	private int readTimeout = 30000;
	private int threadPoolsize = 10;

	// Limit factor is to get more response results to do post-processing
	private float limitFactor;

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public int getReadTimeout() {
		return readTimeout;
	}

	public void setReadTimeout(int readTimeout) {
		this.readTimeout = readTimeout;
	}

	public int getThreadPoolsize() {
		return threadPoolsize;
	}

	public void setThreadPoolsize(int threadPoolsize) {
		this.threadPoolsize = threadPoolsize;
	}

	public float getLimitFactor() {
		return limitFactor;
	}

	public void setLimitFactor(float limitFactor) {
		this.limitFactor = limitFactor;
	}
}
