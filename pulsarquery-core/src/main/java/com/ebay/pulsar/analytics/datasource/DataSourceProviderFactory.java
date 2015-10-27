/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import com.ebay.pulsar.analytics.query.SQLQueryProcessor;

/**
 * 
 * @author mingmwang
 *
 */
public interface DataSourceProviderFactory {
	public boolean validate(DataSourceConfiguration configuration);
	public DataSourceProvider create(DataSourceConfiguration configuration);
	public SQLQueryProcessor queryProcessor();
}
