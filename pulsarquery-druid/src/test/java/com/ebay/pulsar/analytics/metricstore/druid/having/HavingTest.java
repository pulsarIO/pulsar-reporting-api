/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.having;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.HavingType;
import com.ebay.pulsar.analytics.metricstore.druid.having.AndHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.BaseHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.NotHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.OrHaving;

public class HavingTest {


	@Test
	public void test() {
		testAndHaving();
		testNotHaving();
		testOrHaving();
		testGreaterThanHaving();
		testLessThanHaving();
		testEqualToHaving();
	}

	public void testAndHaving() {
		List<BaseHaving> fields = new ArrayList<BaseHaving> ();
		String dim1 = "Aggregate1";
		String dim2 = "Aggregate2";
		String val1 = "Value1";
		String val2 = "Value1";
		EqualToHaving having1 = new EqualToHaving (dim1, val1);
		EqualToHaving having2 = new EqualToHaving (dim2, val2);

		fields.add(having1);
		fields.add(having2);

		AndHaving andHaving = new AndHaving (fields);

		byte[] cacheKey = andHaving.cacheKey();

		String hashCacheKeyExpected = "f9f8bfd569626ac92de64f4a481942b0b2f4221e";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		List<BaseHaving> fieldZ = andHaving.getHavingSpecs();
		EqualToHaving having01 = (EqualToHaving) fieldZ.get(0);
		EqualToHaving having02 = (EqualToHaving) fieldZ.get(1);
		assertEquals("Having1 aggregates NOT Equals", having1.getAggregation(), having01.getAggregation());
		assertEquals("Having1 values NOT Equals", having1.getValue(), having01.getValue());
		assertEquals("Having2 aggregates NOT Equals", having2.getAggregation(), having02.getAggregation());
		assertEquals("Having2 values NOT Equals", having2.getValue(), having02.getValue());

		HavingType type = andHaving.getType();
		assertEquals("AndHaving type NOT Equals", HavingType.and, type);

		// Nothing to do for HavingCacheHelper
		//HavingCacheHelper cacheHelper = new HavingCacheHelper();
		
		List<BaseHaving> fields2 = new ArrayList<BaseHaving> ();
		String dim21 = "Aggregate21";
		String dim22 = "Aggregate22";
		String val21 = "Value21";
		String val22 = "Value21";
		EqualToHaving having21 = new EqualToHaving (dim21, val21);
		EqualToHaving having22 = new EqualToHaving (dim22, val22);

		fields2.add(having21);
		fields2.add(having22);

		AndHaving andHaving2 = new AndHaving (fields2);
		AndHaving andHaving1 = new AndHaving (null);
		assertTrue(!andHaving2.equals(andHaving1));
		assertTrue(!andHaving1.equals(andHaving2));
		andHaving1 = new AndHaving (fields2);
		assertTrue(andHaving2.equals(andHaving1));
		assertTrue(andHaving2.hashCode()==andHaving1.hashCode());
		assertTrue(andHaving2.equals(andHaving2));
		assertTrue(!andHaving2.equals(new NotHaving(having22){
			
		}));
	}

	public void testNotHaving() {
		String dim = "Aggregate";
		String val = "Value";
		EqualToHaving having = new EqualToHaving (dim, val);

		NotHaving notHaving = new NotHaving (having);

		byte[] cacheKey = notHaving.cacheKey();

		String hashCacheKeyExpected = "46c10224ebf198dab14da05b60b14a82589f39ad";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		EqualToHaving having1 = (EqualToHaving) notHaving.getHavingSpec();
		having1 = new EqualToHaving (dim, null);
		NotHaving notHaving1 = new NotHaving (having1);

		byte[] cacheKey1 = notHaving1.cacheKey();

		String hashCacheKeyExpected1 = "74e6b042668b6ee8b0d3d7f49c9ba166a0be2b75";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);

		HavingType type = notHaving.getType();
		assertEquals("HavingType NOT Equals", HavingType.not, type);
		
		String dim2 = "Aggregate2";
		String val2 = "Value2";
		EqualToHaving having2 = new EqualToHaving (dim2, val2);

		NotHaving Having2 = new NotHaving (having2);
		NotHaving Having1 = new NotHaving (null);
		assertTrue(!Having2.equals(Having1));
		assertTrue(!Having1.equals(Having2));
		Having1 = new NotHaving (having2);
		assertTrue(Having2.equals(Having1));
		assertTrue(Having2.hashCode()==Having1.hashCode());
		assertTrue(Having2.equals(Having2));
		assertTrue(!Having2.equals(new NotHaving(having2){
			
		}));
		
	}

	public void testOrHaving() {
		List<BaseHaving> fields = new ArrayList<BaseHaving> ();
		String dim1 = "Aggregate1";
		String dim2 = "Aggregate2";
		String val1 = "Value1";
		String val2 = "Value1";
		EqualToHaving having1 = new EqualToHaving (dim1, val1);
		EqualToHaving having2 = new EqualToHaving (dim2, val2);

		fields.add(having1);
		fields.add(having2);

		OrHaving orHaving = new OrHaving (fields);

		byte[] cacheKey = orHaving.cacheKey();

		String hashCacheKeyExpected = "6b843527795caaa3fee253f709436499e1db561d";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		//List<BaseHaving> fields0 = orHaving.getHavingSpecs();
		// Testing ONLY ONE field
		List<BaseHaving> fields1 = new ArrayList<BaseHaving> ();
		fields1.add(having1);
		OrHaving orHaving1 = new OrHaving (fields1);
		cacheKey = orHaving1.cacheKey();
		String hashCacheKeyExpected1 = "288bc95efa0435b139e6c04ec92d4a02a49d15c3";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);

		HavingType type = orHaving.getType();
		assertEquals("HavingType NOT Equals", HavingType.or, type);
		
		List<BaseHaving> fields2 = new ArrayList<BaseHaving> ();
		String dim21 = "Aggregate21";
		String dim22 = "Aggregate22";
		String val21 = "Value21";
		String val22 = "Value21";
		EqualToHaving having21 = new EqualToHaving (dim21, val21);
		EqualToHaving having22 = new EqualToHaving (dim22, val22);

		fields2.add(having21);
		fields2.add(having22);

		OrHaving Having2 = new OrHaving (fields2);
		OrHaving Having1 = new OrHaving (null);
		assertTrue(!Having2.equals(Having1));
		assertTrue(!Having1.equals(Having2));
		Having1 = new OrHaving (fields2);
		assertTrue(Having2.equals(Having1));
		assertTrue(Having2.hashCode()==Having1.hashCode());
		assertTrue(Having2.equals(Having2));
		assertTrue(!Having2.equals(new NotHaving(having2){
			
		}));
	}

	public void testGreaterThanHaving() {
		String aggregate = "Aggregate";
		String value = "123";
		GreaterThanHaving having = new GreaterThanHaving (aggregate, value);

		String aggregate1 = having.getAggregation();
		String value1 = having.getValue();
		assertEquals("Aggregate NOT Equals", aggregate, aggregate1);
		assertEquals("Value NOT Equals", value, value1);

		byte[] cacheKey = having.cacheKey();

		String hashCacheKeyExpected = "22a85c37b6f950b51f09e5dcb52e2175e898b516";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		GreaterThanHaving having1 = new GreaterThanHaving (aggregate, null);

		byte[] cacheKey1 = having1.cacheKey();

		String hashCacheKeyExpected1 = "92d122aadc2a9bab7a85a04be116e9634611befb";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);

		HavingType type = having.getType();
		assertEquals("HavingType NOT Equals", HavingType.greaterThan, type);
		
		String aggregate2 = "Aggregate";
		String value2 = "123";
		GreaterThanHaving having2 = new GreaterThanHaving (null, null);
		assertTrue(!having2.equals(having));
		assertTrue(!having.equals(having2));
		having2 = new GreaterThanHaving (aggregate2,null);
		assertTrue(!having2.equals(having));
		assertTrue(!having.equals(having2));
		having2 = new GreaterThanHaving (aggregate2,value2);
		assertTrue(having2.equals(having));
		assertTrue(having.equals(having2));
		assertTrue(having2.hashCode()==having.hashCode());
		assertTrue(having2.equals(having2));
		assertTrue(!having2.equals(new NotHaving(having2){
			
		}));
	}

	public void testLessThanHaving() {
		String aggregate = "Aggregate";
		String value = "123";
		LessThanHaving having = new LessThanHaving (aggregate, value);

		String aggregate1 = having.getAggregation();
		String value1 = having.getValue();
		assertEquals("Aggregate NOT Equals", aggregate, aggregate1);
		assertEquals("Value NOT Equals", value, value1);

		byte[] cacheKey = having.cacheKey();

		String hashCacheKeyExpected = "6d73ae7ef7c7d29b751d0223a22030e39dbca31e";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		HavingType type = having.getType();
		assertEquals("HavingType NOT Equals", HavingType.lessThan, type);

		LessThanHaving having1 = new LessThanHaving (aggregate, null);

		byte[] cacheKey1 = having1.cacheKey();

		String hashCacheKeyExpected1 = "4dc371b14fa0e56d4f85634fb751ae3480da4c3a";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);
		
		
		String aggregate2 = "Aggregate";
		String value2 = "123";
		LessThanHaving having2 = new LessThanHaving (null, null);
		assertTrue(!having2.equals(having));
		assertTrue(!having.equals(having2));
		having2 = new LessThanHaving (aggregate2,null);
		assertTrue(!having2.equals(having));
		assertTrue(!having.equals(having2));
		having2 = new LessThanHaving (aggregate2,value2);
		assertTrue(having2.equals(having));
		assertTrue(having.equals(having2));
		assertTrue(having2.hashCode()==having.hashCode());
		assertTrue(having2.equals(having2));
		assertTrue(!having2.equals(new NotHaving(having2){
			
		}));
	}

	public void testEqualToHaving() {
		String dim = "Aggregate";
		String val = "Value";

		EqualToHaving having = new EqualToHaving (dim, val);

		byte[] cacheKey = having.cacheKey();

		String hashCacheKeyExpected = "efab1e8852cb58f8d7aac18bcfedc758be691297";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		HavingType type = having.getType();
		assertEquals("HavingType NOT Equals", HavingType.equalTo, type);
		
		String dim2 = "Aggregate2";
		String val2 = "Value2";
		EqualToHaving having2 = new EqualToHaving (null, null);

		assertTrue(!having2.equals(having));
		assertTrue(!having.equals(having2));
		having2 = new EqualToHaving (dim2,null);
		assertTrue(!having2.equals(having));
		assertTrue(!having.equals(having2));
		having2 = new EqualToHaving (dim2,val2);
		assertTrue(!having2.equals(having));
		assertTrue(!having.equals(having2));
		having2 = new EqualToHaving (dim,val);
		assertTrue(having2.equals(having));
		assertTrue(having2.hashCode()==having.hashCode());
		assertTrue(!having2.equals(new NotHaving(having2){
			
		}));
	}

	public void createAggregators() {

	}
}
