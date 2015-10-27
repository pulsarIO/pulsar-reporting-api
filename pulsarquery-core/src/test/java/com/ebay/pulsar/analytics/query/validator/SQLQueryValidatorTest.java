/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query.validator;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ebay.pulsar.analytics.constants.Constants;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.query.request.SQLRequest;

public class SQLQueryValidatorTest {
	@Test
	public void testValidator() {
		SQLRequest sqlRequest = new SQLRequest();

		sqlRequest.setEndTime("2015-09-9 23:59:59");
		sqlRequest.setGranularity("day");
		sqlRequest.setIntervals("2015-08-20 00:00:00/2015-08-26 23:59:59");
		sqlRequest.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest
				.setSql("select count(clickcount_ag) as \"clickcount_ag\", browserfamily from pulsar_ogmb group by limit 100");
		sqlRequest.setStartTime("2015-09-15 23:59:59");

		SQLRequest sqlRequest2 = new SQLRequest();

		sqlRequest2.setGranularity("day");
		sqlRequest2.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest2
				.setSql("select count(clickcount_ag) as \"clickcount_ag\", browserfamily from pulsar_ogmb group by limit 100");
		sqlRequest2.setCustomTime("today");

		SQLQueryValidator validator = new SQLQueryValidator();
		try {
			validator.validate(sqlRequest);
		} catch (InvalidQueryParameterException ex) {
			assertTrue(false);
		}

		try {
			validator.validate(sqlRequest2);
			fail("Expect InvalidQueryParameterException");
		} catch (InvalidQueryParameterException ex) {
			assertTrue(true);
		}
		
		SQLRequest sqlRequest3 = new SQLRequest();

		sqlRequest3.setGranularity("day");
		sqlRequest3.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest3
				.setSql("select count(clickcount_ag) as \"clickcount_ag\", browserfamily from pulsar_ogmb group by limit 100");
		sqlRequest3.setCustomTime("yesterday");


		try {
			validator.validate(sqlRequest3);
			fail("Expect InvalidQueryParameterException");
		} catch (InvalidQueryParameterException ex) {
			assertTrue(true);
		}
		
		
		SQLRequest sqlRequest5 = new SQLRequest();

		sqlRequest5.setGranularity("day");
		sqlRequest5.setStartTime("test");
		sqlRequest5.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest5
				.setSql("select count(clickcount_ag) as \"clickcount_ag\", browserfamily from pulsar_ogmb group by limit 100");
		sqlRequest5.setCustomTime("test");


		try {
			validator.validate(sqlRequest5);
			fail("Expect InvalidQueryParameterException");
		} catch (InvalidQueryParameterException ex) {
			assertTrue(true);
		}
		
		SQLRequest sqlRequest6 = new SQLRequest();

		sqlRequest6.setEndTime("2015-09-9 23:59:59");
		sqlRequest6.setGranularity("day");
		sqlRequest6.setIntervals("/2015-08-26 23:59:59");
		sqlRequest6.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest6
				.setSql("select count(clickcount_ag) as \"clickcount_ag\", browserfamily from pulsar_ogmb group by limit 100");
		sqlRequest.setStartTime(null);


		try {
			validator.validate(sqlRequest6);
			fail("Expect InvalidQueryParameterException");
		} catch (InvalidQueryParameterException ex) {
			assertTrue(true);
		}
		
		SQLRequest sqlRequest7 = new SQLRequest();

		sqlRequest7.setEndTime(null);
		sqlRequest7.setGranularity("day");
		sqlRequest7.setIntervals("2015-08-20 00:00:00");
		sqlRequest7.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest7
				.setSql("select count(clickcount_ag) as \"clickcount_ag\", browserfamily from pulsar_ogmb group by limit 100");
		sqlRequest.setStartTime("2015-09-9 23:59:59");


		try {
			validator.validate(sqlRequest7);
			fail("Expect InvalidQueryParameterException");
		} catch (InvalidQueryParameterException ex) {
			assertTrue(true);
		}
		
		SQLRequest sqlRequest8 = new SQLRequest();

		sqlRequest8.setEndTime(null);
		sqlRequest8.setGranularity("day");
		sqlRequest8.setIntervals("2015-08-20 00:00:00/2015-08-10 00:00:00");
		sqlRequest8.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest8
				.setSql("select count(clickcount_ag) as \"clickcount_ag\", browserfamily from pulsar_ogmb group by limit 100");
		sqlRequest.setStartTime("2015-09-9 23:59:59");


		try {
			validator.validate(sqlRequest8);
			fail("Expect InvalidQueryParameterException");
		} catch (InvalidQueryParameterException ex) {
			assertTrue(true);
		}
		
		SQLRequest sqlRequest9 = new SQLRequest();

		sqlRequest9.setEndTime(null);
		sqlRequest9.setGranularity("day");
		sqlRequest9.setIntervals("3015-08-20 00:00:00/3015-08-30 00:00:00");
		sqlRequest9.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest9
				.setSql("select count(clickcount_ag) as \"clickcount_ag\", browserfamily from pulsar_ogmb group by limit 100");
		sqlRequest.setStartTime("2015-09-9 23:59:59");


		try {
			validator.validate(sqlRequest9);
			fail("Expect InvalidQueryParameterException");
		} catch (InvalidQueryParameterException ex) {
			assertTrue(true);
		}
		
		SQLRequest sqlRequest10 = new SQLRequest();

		sqlRequest10.setEndTime(null);
		sqlRequest10.setGranularity("day");
		sqlRequest10.setIntervals("3015-08-20 00:00:00/3015-08-30 00:00:00");
		sqlRequest10.setNamespace(Constants.RequestNameSpace.sql);
		sqlRequest.setStartTime("2015-09-9 23:59:59");


		try {
			validator.validate(sqlRequest10);
			fail("Expect InvalidQueryParameterException");
		} catch (InvalidQueryParameterException ex) {
			assertTrue(true);
		}

	}
}
