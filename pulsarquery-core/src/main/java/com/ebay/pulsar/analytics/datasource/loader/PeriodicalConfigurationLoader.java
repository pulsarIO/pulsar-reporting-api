/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource.loader;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ebay.pulsar.analytics.util.PulsarQueryScheduler;

/**
 * 
 * @author mingmwang
 *
 */
public class PeriodicalConfigurationLoader implements DataSourceConfigurationLoader {
	
	private static final Long ONE_MINUTE = 60 * 1000L;
	
	public PeriodicalConfigurationLoader(final DataSourceConfigurationLoader loader) {
		ScheduledExecutorService scheduler = PulsarQueryScheduler.getScheduler();
		scheduler.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				try{
					loader.load();
				}catch(Exception t){
				}
			}
		}, 0, ONE_MINUTE, TimeUnit.MILLISECONDS);
	}

	@Override
	public void load() {
	}
}
