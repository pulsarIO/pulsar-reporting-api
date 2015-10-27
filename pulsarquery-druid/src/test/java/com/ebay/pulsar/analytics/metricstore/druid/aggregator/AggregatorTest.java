/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.aggregator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.AggregatorType;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.SelectorFilter;

public class AggregatorTest {

	@Before
	public void setup() throws Exception {
		createAggregators();
	}

	@After
	public void after() throws Exception {
	}
	@Test
	public void test() {
		testCardinalityAggregator();
		testCountAggregator();
		testDoubleSumAggregator();
		testFilteredAggregator();
		testHyperUniqueAggregator();
		testLongSumAggregator();
		testMaxAggregator();
		testMinAggregator();
	}

	public void testCardinalityAggregator() {
		String aggregatorName = "CardAggrTest";
		List<String> fieldNames = new ArrayList<String> ();
		String field1 = "Field1";
		String field2 = "Field2";
		fieldNames.add(field1);
		fieldNames.add(field2);

		CardinalityAggregator cardAggr = new CardinalityAggregator (aggregatorName, fieldNames);

		cardAggr.setByRow(true);
		boolean byRow = cardAggr.getByRow();
		assertTrue("Property byRow must be TRUE", byRow);
		List<String> fieldNamesGot = cardAggr.getFieldNames();
		assertEquals("1st Field must be 'Field1'", field1, fieldNamesGot.get(0));
		assertEquals("2nd Field must be 'Field2'", field2, fieldNamesGot.get(1));

		byte[] cacheKey = cardAggr.cacheKey();

		String hashCacheKeyExpected = "ecdec9686d4e3f4e137ae430691c5becc2a21192";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		AggregatorType type = cardAggr.getType();
		assertEquals("Type NOT Equals", AggregatorType.cardinality, type);
		
		String aggregatorName2 = "CardAggrTest2";
		List<String> fieldNames2 = new ArrayList<String> ();
		String field21 = "Field21";
		String field22 = "Field22";
		fieldNames2.add(field21);
		fieldNames2.add(field22);

		CardinalityAggregator agg2 = new CardinalityAggregator (aggregatorName2, fieldNames2);
		CardinalityAggregator agg3 = new CardinalityAggregator (aggregatorName2, fieldNames2);
		assertTrue(agg2.hashCode()==agg3.hashCode());
		assertTrue(agg2.equals(agg3));
		assertTrue(agg2.equals(agg2));
		agg3.setByRow(true);
		assertTrue(!agg2.equals(agg3));
		assertTrue(!agg3.equals(agg2));
		agg3.setByRow(false);
		assertTrue(agg2.equals(agg2));
		assertTrue(!agg2.equals(cardAggr));
		assertTrue(!agg2.equals(null));
		agg3= new CardinalityAggregator (aggregatorName2, null);
		assertTrue(!agg2.equals(agg3));
		assertTrue(!agg3.equals(agg2));
		assertTrue(!agg2.equals(new Serializable(){
			private static final long serialVersionUID = 1L;}));
		assertTrue(!new Serializable(){
			private static final long serialVersionUID = 1L;}.equals(agg2));
		
		
	}

	public void testCountAggregator() {
		String aggregatorName = "CountAggrTest";

		CountAggregator countAggr = new CountAggregator (aggregatorName);

		byte[] cacheKey = countAggr.cacheKey();

		String hashCacheKeyExpected = "36f680f51c5c6c258211a80db8498fda80c748f9";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		AggregatorType type = countAggr.getType();
		assertEquals("Type NOT Equals", AggregatorType.count, type);
		
		String aggregatorName2 = "CardAggrTest2";
		CountAggregator agg2 = new CountAggregator (aggregatorName2);
		CountAggregator agg3 = new CountAggregator (aggregatorName2);
		assertTrue(agg2.hashCode()==agg3.hashCode());
		assertTrue(agg2.equals(agg2));
		assertTrue(agg2.equals(agg3));
		assertTrue(!agg2.equals(null));
		agg3= new CountAggregator (aggregatorName);
		assertTrue(!agg2.equals(agg3));
		assertTrue(!agg3.equals(agg2));
		assertTrue(!agg2.equals(new Serializable(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;}));
		assertTrue(!new Serializable(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;}.equals(agg2));
		
	}

	public void testDoubleSumAggregator() {
		String aggregatorName = "DblSumAggrTest";
		String fieldName = "FieldName";

		DoubleSumAggregator dblSumAggr = new DoubleSumAggregator (aggregatorName, fieldName);

		String fieldNameGot = dblSumAggr.getFieldName();
		assertEquals("FieldName must be 'FieldName'", fieldName, fieldNameGot);

		byte[] cacheKey = dblSumAggr.cacheKey();

		String hashCacheKeyExpected = "1b041440da6c3182cba79137afc569069f555c46";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		
		String aggregatorName2 = "DblSumAggrTest2";
		String fieldName2 = "FieldName2";
		DoubleSumAggregator agg2 = new DoubleSumAggregator (aggregatorName2,fieldName2);
		DoubleSumAggregator agg3 = new DoubleSumAggregator (aggregatorName2,fieldName2);
		assertTrue(agg2.hashCode()==agg3.hashCode());
		assertTrue(agg2.equals(agg2));
		assertTrue(agg2.equals(agg3));
		assertTrue(!agg2.equals(null));
		agg3= new DoubleSumAggregator (aggregatorName,fieldName2);
		assertTrue(!agg2.equals(agg3));
		assertTrue(!agg3.equals(agg2));
		agg3= new DoubleSumAggregator (aggregatorName2,fieldName);
		assertTrue(!agg2.equals(agg3));
		assertTrue(!agg3.equals(agg2));
		agg3= new DoubleSumAggregator (null,fieldName);
		assertTrue(!agg2.equals(agg3));
		assertTrue(!agg3.equals(agg2));
		agg3= new DoubleSumAggregator (aggregatorName2,null);
		assertTrue(!agg2.equals(agg3));
		assertTrue(!agg3.equals(agg2));
		assertTrue(!agg2.equals(new CountAggregator (aggregatorName2)));
	}

	public void testFilteredAggregator() {
		String filteredAggregatorName = "FilteredAggrTest";
		String dimension = "columnName";
		String value = "columnValue";
		BaseFilter filter = new SelectorFilter (dimension, value);

		String baseAggregatorName = "LongSumAggrTest";
		String fieldName = "FieldName";

		LongSumAggregator longSumAggr = new LongSumAggregator (baseAggregatorName, fieldName);
		FilteredAggregator filteredAggr = new FilteredAggregator (filteredAggregatorName, filter, longSumAggr);

		SelectorFilter filterGot = (SelectorFilter) filteredAggr.getFilter();
		String dimensionGot = filterGot.getDimension();
		String valueGot = filterGot.getValue();
		assertEquals("BaseFilter dimension must be 'columnName'", dimension, dimensionGot);
		assertEquals("BaseFilter value must be 'columnValue'", value, valueGot);

		LongSumAggregator longSumAggrGot = (LongSumAggregator) filteredAggr.getAggregator();
		longSumAggrGot.getName();

		byte[] cacheKey = filteredAggr.cacheKey();

		String hashCacheKeyExpected = "be9b174d517e49eff7c366f4225f72a300ae65b2";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		
		String aggregatorName2 = "DblSumAggrTest2";
		//String fieldName2 = "FieldName2";
		BaseFilter filter2 = new SelectorFilter (dimension, value);
		LongSumAggregator long2 = new LongSumAggregator (baseAggregatorName, fieldName);
		FilteredAggregator agg2 = new FilteredAggregator (aggregatorName2, filter2, long2);
		FilteredAggregator agg3 = new FilteredAggregator (aggregatorName2,filter2,long2);
		assertTrue(agg2.hashCode()==agg3.hashCode());
		assertTrue(agg2.equals(agg2));
		assertTrue(agg2.equals(agg3));
		assertTrue(!agg2.equals(null));
		FilteredAggregator agg4 = new FilteredAggregator (null,filter2,long2);
		assertTrue(!agg2.equals(agg4));assertTrue(!agg4.equals(agg2));
		FilteredAggregator agg5 = new FilteredAggregator (aggregatorName2,null,long2);
		assertTrue(!agg2.equals(agg5));assertTrue(!agg5.equals(agg2));
		FilteredAggregator agg6 = new FilteredAggregator (aggregatorName2,filter2,null);
		assertTrue(!agg2.equals(agg6));assertTrue(!agg6.equals(agg2));
		assertTrue(!agg2.equals(new CountAggregator (aggregatorName2)));
	}

	public void testHyperUniqueAggregator() {
		String aggregatorName = "HyperUniqueAggrTest";
		String fieldName = "FieldName";

		HyperUniqueAggregator hyperUniqueAggr = new HyperUniqueAggregator (aggregatorName, fieldName);

		String fieldNameGot = hyperUniqueAggr.getFieldName();
		assertEquals("FieldName must be 'FieldName'", fieldName, fieldNameGot);

		String aggregatorNameGot = hyperUniqueAggr.getName();
		assertEquals("Name must be 'HyperUniqueAggrTest'", aggregatorName, aggregatorNameGot);

		byte[] cacheKey = hyperUniqueAggr.cacheKey();

		String hashCacheKeyExpected = "0f7527d25c5d69a8c58a403632b06aa23394abf7";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		
		String aggregatorName2 = "HyperUniqueAggrTest2";
		String fieldName2 = "FieldName2";
		HyperUniqueAggregator agg2 = new HyperUniqueAggregator (aggregatorName2, fieldName2);
		HyperUniqueAggregator agg3 = new HyperUniqueAggregator (null, null);
		assertTrue(!agg3.equals(agg2));assertTrue(!agg2.equals(agg3));
		HyperUniqueAggregator agg4 = new HyperUniqueAggregator (aggregatorName2, null);
		assertTrue(!agg4.equals(agg2));assertTrue(!agg4.equals(agg3));
		HyperUniqueAggregator agg5 = new HyperUniqueAggregator (aggregatorName2, fieldName2);
		assertTrue(agg5.equals(agg2));
		assertTrue(agg2.equals(agg5));
		assertTrue(!agg2.equals(new CountAggregator (aggregatorName2)));
		assertTrue(agg5.hashCode()==agg2.hashCode());
	}

	public void testLongSumAggregator() {
		String aggregatorName = "LongSumAggrTest";
		String fieldName = "FieldName";

		LongSumAggregator longSumAggr = new LongSumAggregator (aggregatorName, fieldName);

		String fieldNameGot = longSumAggr.getFieldName();
		assertEquals("FieldName must be 'FieldName'", fieldName, fieldNameGot);

		String aggregatorNameGot = longSumAggr.getName();
		assertEquals("Name must be 'LongSumAggrTest'", aggregatorName, aggregatorNameGot);

		byte[] cacheKey = longSumAggr.cacheKey();

		String hashCacheKeyExpected = "fbad1dbb9a9ab3c6c722b9aa495a23e7390aff38";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		
		String aggregatorName2 = "LongSumAggrTest2";
		String fieldName2 = "FieldName2";
		LongSumAggregator agg2 = new LongSumAggregator (aggregatorName2, fieldName2);
		LongSumAggregator agg3 = new LongSumAggregator (null, null);
		assertTrue(!agg3.equals(agg2));assertTrue(!agg2.equals(agg3));
		LongSumAggregator agg4 = new LongSumAggregator (aggregatorName2, null);
		assertTrue(!agg4.equals(agg2));assertTrue(!agg4.equals(agg3));
		LongSumAggregator agg5 = new LongSumAggregator (aggregatorName2, fieldName2);
		assertTrue(agg5.equals(agg2));
		assertTrue(agg2.equals(agg5));
		assertTrue(!agg2.equals(new CountAggregator (aggregatorName2)));
		assertTrue(agg5.hashCode()==agg2.hashCode());
	}

	public void testMaxAggregator() {
		String aggregatorName = "MaxAggrTest";
		String fieldName = "FieldName";

		MaxAggregator maxAggr = new MaxAggregator (aggregatorName, fieldName);

		String fieldNameGot = maxAggr.getFieldName();
		assertEquals("FieldName must be 'FieldName'", fieldName, fieldNameGot);

		String aggregatorNameGot = maxAggr.getName();
		assertEquals("Name must be 'MaxAggrTest'", aggregatorName, aggregatorNameGot);

		byte[] cacheKey = maxAggr.cacheKey();

		String hashCacheKeyExpected = "8f442eb6f8ed1a9676b810c0d4625b580b5d28e8";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		
		String aggregatorName2 = "MaxAggrTest2";
		String fieldName2 = "FieldName2";
		MaxAggregator agg2 = new MaxAggregator (aggregatorName2, fieldName2);
		MaxAggregator agg3 = new MaxAggregator (null, null);
		assertTrue(!agg3.equals(agg2));assertTrue(!agg2.equals(agg3));
		MaxAggregator agg4 = new MaxAggregator (aggregatorName2, null);
		assertTrue(!agg4.equals(agg2));assertTrue(!agg4.equals(agg3));
		MaxAggregator agg5 = new MaxAggregator (aggregatorName2, fieldName2);
		assertTrue(agg5.equals(agg2));
		assertTrue(agg2.equals(agg5));
		assertTrue(!agg2.equals(new CountAggregator (aggregatorName2)));
		assertTrue(agg5.hashCode()==agg2.hashCode());
	}

	public void testMinAggregator() {
		String aggregatorName = "MinAggrTest";
		String fieldName = "FieldName";

		MinAggregator minAggr = new MinAggregator (aggregatorName, fieldName);

		String fieldNameGot = minAggr.getFieldName();
		assertEquals("FieldName must be 'FieldName'", fieldName, fieldNameGot);

		String aggregatorNameGot = minAggr.getName();
		assertEquals("Name must be 'MinAggrTest'", aggregatorName, aggregatorNameGot);

		byte[] cacheKey = minAggr.cacheKey();

		String hashCacheKeyExpected = "261dd9d040a946b9cc554a7150ac3793d2dfa662";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		
		String aggregatorName2 = "MinAggrTest2";
		String fieldName2 = "FieldName2";
		MinAggregator agg2 = new MinAggregator (aggregatorName2, fieldName2);
		MinAggregator agg3 = new MinAggregator (null, null);
		assertTrue(!agg3.equals(agg2));assertTrue(!agg2.equals(agg3));
		MinAggregator agg4 = new MinAggregator (aggregatorName2, null);
		assertTrue(!agg4.equals(agg2));assertTrue(!agg4.equals(agg3));
		MinAggregator agg5 = new MinAggregator (aggregatorName2, fieldName2);
		assertTrue(agg5.equals(agg2));
		assertTrue(agg2.equals(agg5));
		assertTrue(!agg2.equals(new CountAggregator (aggregatorName2)));
		assertTrue(agg5.hashCode()==agg2.hashCode());
	}

	public void createAggregators() {

	}
}
