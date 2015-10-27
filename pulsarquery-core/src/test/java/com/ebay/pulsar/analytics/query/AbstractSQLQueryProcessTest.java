/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;

import org.junit.Test;
import org.mockito.Mockito;

import com.ebay.pulsar.analytics.constants.Constants;
import com.ebay.pulsar.analytics.query.request.SQLRequest;
import com.ebay.pulsar.analytics.query.response.TraceAbleResponse;
import com.ebay.pulsar.analytics.query.sql.SQLTranslator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class AbstractSQLQueryProcessTest extends AbstractSQLQueryProcessor{
	@Test
	public void testProcess(){
		SQLRequest sqlRequest = new SQLRequest();

		sqlRequest.setEndTime("2015-09-9 23:59:59");
		sqlRequest.setGranularity("day");
		sqlRequest.setIntervals("2015-08-20 00:00:00/2015-08-26 23:59:59");
		sqlRequest.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest
				.setSql("select count(clickcount_ag) as \"clickcount_ag\", testDim from tabletest group by testDim limit 100");
		sqlRequest.setStartTime("2015-09-15 23:59:59");
		
		assertEquals(sqlRequest.getGranularity(),prepareQuery(sqlRequest,"druid").getGranularity());
	}

	@Override
	public SQLTranslator getSqlTranslator() {
		SQLTranslator sqlTranslator = Mockito.mock(SQLTranslator.class,
				Mockito.CALLS_REAL_METHODS);
		return sqlTranslator;
	}

	@Override
	public TraceAbleResponse doSQLQuery(SQLQueryContext queryContext)
			throws JsonParseException, JsonMappingException, IOException,
			ParseException {
		return null;
	}

}
