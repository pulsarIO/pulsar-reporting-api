/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query;

import java.io.IOException;
import java.text.ParseException;

import com.ebay.pulsar.analytics.query.request.BaseRequest;
import com.ebay.pulsar.analytics.query.response.TraceAbleResponse;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * Rest and SQL query processor
 * 
 * @author mingmwang
 *
 */
public interface RestQueryProcessor extends SQLQueryProcessor {
	TraceAbleResponse executeRestQuery(BaseRequest req) throws JsonParseException, JsonMappingException, IOException, ParseException;
}
