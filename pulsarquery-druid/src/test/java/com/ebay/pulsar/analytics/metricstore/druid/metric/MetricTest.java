/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.metric;

import static org.junit.Assert.*;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.pulsar.analytics.metricstore.druid.metric.AlphaNumericMetric;
import com.ebay.pulsar.analytics.metricstore.druid.metric.InvertedMetric;
import com.ebay.pulsar.analytics.metricstore.druid.metric.LexicographicMetric;
import com.ebay.pulsar.analytics.metricstore.druid.metric.NumericMetric;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.MetricType;

public class MetricTest {

	@Before
	public void setup() throws Exception {
	}

	@After
	public void after() throws Exception {
	}

	@Test
	public void test() {
		testAlphaNumericMetric();
		testInvertedMetric();
		testLexicographicMetric();
		testNumericMetric();
	}

	public void testAlphaNumericMetric() {
		String previousStop = "AlphaNumericTest";

		AlphaNumericMetric alphaNumericMetric = new AlphaNumericMetric (previousStop);

		String previousStopGot = alphaNumericMetric.getPreviousStop();
		assertEquals("FieldName must be 'FieldName'", previousStop, previousStopGot);

		byte[] cacheKey = alphaNumericMetric.cacheKey();

		String hashCacheKeyExpected = "3c5a495be73819bae712180d05433563229190df";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		MetricType type = alphaNumericMetric.getType();
		assertEquals("Type NOT Equals", MetricType.alphaNumeric, type);
		
		String previousStop2 = "AlphaNumericTest2";
		AlphaNumericMetric metric2 = new AlphaNumericMetric (null);
		assertTrue(!metric2.equals(alphaNumericMetric));
		assertTrue(!alphaNumericMetric.equals(metric2));
		metric2 = new AlphaNumericMetric (previousStop2);
		assertTrue(!metric2.equals(alphaNumericMetric));
		assertTrue(!alphaNumericMetric.equals(metric2));
		
//		metric2 = new AlphaNumericMetric (previousStop);
//		assertTrue(metric2.equals(alphaNumericMetric));
//		assertTrue(alphaNumericMetric.equals(metric2));
//		
//		assertTrue(alphaNumericMetric.hashCode()==metric2.hashCode());
//		
//		assertTrue(!metric2.equals(new NumericMetric("abc"){
//			
//		}));
	}

	public void testInvertedMetric() {
		String metricName = "InvertedMetricTest";

		NumericMetric numericMetric = new NumericMetric (metricName);
		InvertedMetric invertedMetric = new InvertedMetric (numericMetric);

		NumericMetric numericMetricGot = (NumericMetric) invertedMetric.getMetric();
		assertEquals("Metric Names NOT Equals", numericMetric.getMetric(), numericMetricGot.getMetric());

		byte[] cacheKey = invertedMetric.cacheKey();

		String hashCacheKeyExpected = "6efcc0886129ea68f8cc255aa390a93d55aecc3c";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		MetricType type = invertedMetric.getType();
		assertEquals("Type NOT Equals", MetricType.inverted, type);
	}

	public void testLexicographicMetric() {
		String previousStop = "LexicoGraphicMetricTest";

		LexicographicMetric lexicoGraphMetric = new LexicographicMetric (previousStop);

		String previousStopGot = lexicoGraphMetric.getPreviousStop();
		assertEquals("FieldName must be 'FieldName'", previousStop, previousStopGot);

		byte[] cacheKey = lexicoGraphMetric.cacheKey();

		String hashCacheKeyExpected = "d782ebbc81c4e652f2928f2ddc4e437fdffdfd6d";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		LexicographicMetric lexicoGraphMetric1 = new LexicographicMetric (null);

		byte[] cacheKey1 = lexicoGraphMetric1.cacheKey();

		String hashCacheKeyExpected1 = "bf8b4530d8d246dd74ac53a13471bba17941dff7";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);

		MetricType type = lexicoGraphMetric.getType();
		assertEquals("Type NOT Equals", MetricType.lexicographic, type);
	}

	public void testNumericMetric() {
		String metricName = "NumericMetricTest";
		NumericMetric numericMetric = new NumericMetric (metricName);

		String metricNameGot = numericMetric.getMetric();
		assertEquals("FieldName must be 'FieldName'", metricName, metricNameGot);

		byte[] cacheKey = numericMetric.cacheKey();

		String hashCacheKeyExpected = "10f07d4d5d2d407dd6b11efb776fa6fe25eb8811";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		MetricType type = numericMetric.getType();
		assertEquals("Type NOT Equals", MetricType.numeric, type);
	}

}
