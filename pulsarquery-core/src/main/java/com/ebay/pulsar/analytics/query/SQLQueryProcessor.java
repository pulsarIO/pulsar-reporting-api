/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query;

import java.io.IOException;
import java.text.ParseException;

import com.ebay.pulsar.analytics.query.request.BaseSQLRequest;
import com.ebay.pulsar.analytics.query.response.TraceAbleResponse;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
/**
 * Implement this to do the real query in each dataSource.
 * 
 * @author mingmwang
 * 
 **/
public interface SQLQueryProcessor {
	public TraceAbleResponse executeQuery(BaseSQLRequest req,  String dataSourceName) throws JsonParseException, JsonMappingException, IOException, ParseException;
}
