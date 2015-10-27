/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.postaggregator;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.ArithmeticPostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.PostAggregatorType;

public class PostAggregatorTest {


	@Test
	public void test() {
		testArithmeticPostAggregator();
		testConstantPostAggregator();
		testFieldAccessPostAggregator();
		testHyperUniqueCardinalityPostAggregator();
	}

	public void testArithmeticPostAggregator() {
		String postAggrName = "ArithmeticTest";
		List<BasePostAggregator> fields = new ArrayList<BasePostAggregator> ();
		String funcName = "+";

		String postAggrName1 = "PostAggrName1";
		String postAggrName2 = "PostAggrName2";
		String fieldName1 = "FieldName1";
		String fieldName2 = "FieldName2";
		FieldAccessorPostAggregator field1 = new FieldAccessorPostAggregator(postAggrName1, fieldName1);
		FieldAccessorPostAggregator field2 = new FieldAccessorPostAggregator(postAggrName2, fieldName2);
		fields.add(field1);
		fields.add(field2);

		ArithmeticPostAggregator arithPostAggr = new ArithmeticPostAggregator (postAggrName, funcName, fields);

		List<BasePostAggregator> fieldsGot = arithPostAggr.getFields();
		FieldAccessorPostAggregator field01 = (FieldAccessorPostAggregator) fieldsGot.get(0);
		FieldAccessorPostAggregator field02 = (FieldAccessorPostAggregator) fieldsGot.get(1);
		assertEquals("1st Field Names NOT EQUAL'", field1.getName(), field01.getName());
		assertEquals("2nd Field Names NOT EQUAL'", field2.getName(), field02.getName());

		String funcName1 = arithPostAggr.getFn();
		assertEquals("Func Names NOT EQUAL'", funcName, funcName1);

		byte[] cacheKey = arithPostAggr.cacheKey();

		String hashCacheKeyExpected = "d86ce360fa6ee1fc3fe211beacc8df8f151fa5db";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		String postAggrName01 = "ArithmeticTest01";
		List<BasePostAggregator> fields1 = new ArrayList<BasePostAggregator> ();
		fields1.add(field1);
		ArithmeticPostAggregator arithPostAggr1 = new ArithmeticPostAggregator (postAggrName01, funcName, fields1);

		byte[] cacheKey1 = arithPostAggr1.cacheKey();

		String hashCacheKeyExpected1 = "4dd11e0f70f0d89384d0e5a6e373c6ea722b7895";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);

		PostAggregatorType type = arithPostAggr.getType();
		assertEquals("Type NOT Equals", PostAggregatorType.arithmetic, type);
		//PostAggregatorCacheHelper cacheHelper = new PostAggregatorCacheHelper ();
		
//		String postAggrName = "ArithmeticTest";
//		List<BasePostAggregator> fields = new ArrayList<BasePostAggregator> ();
//		String funcName = "+";
//
//		String postAggrName1 = "PostAggrName1";
//		String postAggrName2 = "PostAggrName2";
//		String fieldName1 = "FieldName1";
//		String fieldName2 = "FieldName2";
//		FieldAccessorPostAggregator field1 = new FieldAccessorPostAggregator(postAggrName1, fieldName1);
//		FieldAccessorPostAggregator field2 = new FieldAccessorPostAggregator(postAggrName2, fieldName2);
//		fields.add(field1);
//		fields.add(field2);
		ArithmeticPostAggregator postAggr0 = new ArithmeticPostAggregator (postAggrName, funcName, fields);
		assertTrue(postAggr0.equals(arithPostAggr));assertTrue(postAggr0.equals(postAggr0));
		ArithmeticPostAggregator postAggr = new ArithmeticPostAggregator (null, null);
		assertTrue(!postAggr.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr));
		postAggr = new ArithmeticPostAggregator (postAggrName, null);
		assertTrue(!postAggr.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr));
		postAggr = new ArithmeticPostAggregator (postAggrName, funcName);
		assertTrue(!postAggr.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr));
		postAggr = new ArithmeticPostAggregator (postAggrName, funcName,fields);
		assertTrue(postAggr.equals(postAggr0));assertTrue(postAggr0.equals(postAggr));
		
		assertTrue(postAggr.hashCode()==postAggr0.hashCode());
		assertTrue(!postAggr.equals(new Object()));
	}

	public void testConstantPostAggregator() {
		String postAggrName = "ConstantPostAggrTest";
		Long valueLong = 1001L;

		ConstantPostAggregator constantPostAggr = new ConstantPostAggregator (postAggrName, valueLong);

		byte[] cacheKeyLong = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedL = "c14b848f4656cea09df71e42c7c989132ba8aa6f";
		String hashCacheKeyGotL = DigestUtils.shaHex(cacheKeyLong);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedL, hashCacheKeyGotL);

		Integer valueInt = 1002;
		constantPostAggr = new ConstantPostAggregator (postAggrName, valueInt);

		byte[] cacheKeyInt = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedI = "580228bb63a75f7b07c84dcf2099b0cdaec085ce";
		String hashCacheKeyGotI = DigestUtils.shaHex(cacheKeyInt);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedI, hashCacheKeyGotI);

		Integer valueGot = (Integer) constantPostAggr.getValue();
		assertEquals("Values NOT Equals", valueInt, valueGot);

		Short valueShort = 1003;
		constantPostAggr = new ConstantPostAggregator (postAggrName, valueShort);

		byte[] cacheKeyShort = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedS = "fd73cdc5e039a24a5441b19d158edeb015f10f0c";
		String hashCacheKeyGotS = DigestUtils.shaHex(cacheKeyShort);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedS, hashCacheKeyGotS);

		Byte valueByte = 'a';
		constantPostAggr = new ConstantPostAggregator (postAggrName, valueByte);

		byte[] cacheKeyByte = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedB = "2c0c2af089b139171def55e59d680259fd259f55";
		String hashCacheKeyGotB = DigestUtils.shaHex(cacheKeyByte);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedB, hashCacheKeyGotB);

		Double valueDouble = (double) 1004;
		constantPostAggr = new ConstantPostAggregator (postAggrName, valueDouble);

		byte[] cacheKeyDouble = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedD = "5e1cc5c3150172c4aa8866a6a82b1c4f6f6c200d";
		String hashCacheKeyGotD = DigestUtils.shaHex(cacheKeyDouble);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedD, hashCacheKeyGotD);

		Float valueFloat = (float) 1005;
		constantPostAggr = new ConstantPostAggregator (postAggrName, valueFloat);

		byte[] cacheKeyFloat = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedF = "8c45dc9f4dc79a2aadedf5a48482ae30a6146b02";
		String hashCacheKeyGotF = DigestUtils.shaHex(cacheKeyFloat);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedF, hashCacheKeyGotF);

		BigDecimal valueBigDec = new BigDecimal (1006);
		constantPostAggr = new ConstantPostAggregator (postAggrName, valueBigDec);

		byte[] cacheKeyBigDec = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedBD = "067646bcb514d58a7f9649e2b3101ebd093f8726";
		String hashCacheKeyGotBD = DigestUtils.shaHex(cacheKeyBigDec);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedBD, hashCacheKeyGotBD);

		BigInteger valueBigInt = new BigInteger ("1007");
		constantPostAggr = new ConstantPostAggregator (postAggrName, valueBigInt);

		byte[] cacheKeyBigInt = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedBI = "b278c39b38033eb31d5cddc033cc74f7872e968b";
		String hashCacheKeyGotBI = DigestUtils.shaHex(cacheKeyBigInt);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedBI, hashCacheKeyGotBI);

		AtomicInteger valueAtomInt = new AtomicInteger (1008);
		constantPostAggr = new ConstantPostAggregator (postAggrName, valueAtomInt);

		byte[] cacheKeyAtomInt = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedAI = "45f2a9cf39e5c2ce13180b800b26d5fe09d4553e";
		String hashCacheKeyGotAI = DigestUtils.shaHex(cacheKeyAtomInt);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedAI, hashCacheKeyGotAI);

		AtomicLong valueAtomLong = new AtomicLong (1009L);
		constantPostAggr = new ConstantPostAggregator (postAggrName, valueAtomLong);

		byte[] cacheKeyAtomLong = constantPostAggr.cacheKey();

		String hashCacheKeyExpectedAL = "071e76f10db52ee7f98552e53d974f1879499428";
		String hashCacheKeyGotAL = DigestUtils.shaHex(cacheKeyAtomLong);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpectedAL, hashCacheKeyGotAL);

		PostAggregatorType type = constantPostAggr.getType();
		assertEquals("Type NOT Equals", PostAggregatorType.constant, type);
		
		ConstantPostAggregator postAggr0 = new ConstantPostAggregator(postAggrName, 1001L);
		assertTrue(postAggr0.equals(postAggr0));
		ConstantPostAggregator postAggr = new ConstantPostAggregator (null, null);
		assertTrue(!postAggr.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr));
		postAggr = new ConstantPostAggregator (postAggrName, null);
		assertTrue(!postAggr.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr));
		postAggr = new ConstantPostAggregator (postAggrName, valueFloat);
		assertTrue(!postAggr.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr));
		postAggr = new ConstantPostAggregator (postAggrName, 1001L);
		assertTrue(postAggr.equals(postAggr0));assertTrue(postAggr0.equals(postAggr));
		
		assertTrue(postAggr.hashCode()==postAggr0.hashCode());
		assertTrue(!postAggr.equals(new Object()));
		
	}

	public void testFieldAccessPostAggregator() {
		String postAggrName = "FieldAccessTest";

		String fieldName = "FieldName";
		FieldAccessorPostAggregator postAggr = new FieldAccessorPostAggregator(postAggrName, fieldName);

		byte[] cacheKey = postAggr.cacheKey();

		String hashCacheKeyExpected = "22c95e82dfec8270f675c56caca53c535c043ca7";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		String fieldName1 = postAggr.getFieldName();
		assertEquals("FieldNames NOT Equals", fieldName, fieldName1);

		PostAggregatorType type = postAggr.getType();
		assertEquals("Type NOT Equals", PostAggregatorType.fieldAccess, type);
		
		String postAggrName2 = "FieldAccessTest2";
		String fieldName2 = "FieldName2";
		FieldAccessorPostAggregator postAggr0 = new FieldAccessorPostAggregator(postAggrName, fieldName);
		assertTrue(postAggr0.equals(postAggr0));
		assertTrue(postAggr0.equals(postAggr));
		FieldAccessorPostAggregator postAggr1 = new FieldAccessorPostAggregator (null, null);
		assertTrue(!postAggr1.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr1));
		postAggr1 = new FieldAccessorPostAggregator (postAggrName, null);
		assertTrue(!postAggr1.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr1));
		postAggr1 = new FieldAccessorPostAggregator (postAggrName, fieldName2);
		assertTrue(!postAggr1.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr1));
		postAggr1 = new FieldAccessorPostAggregator (postAggrName2, fieldName);
		assertTrue(!postAggr1.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr1));
		
		postAggr1 = new FieldAccessorPostAggregator (postAggrName, fieldName);
		assertTrue(postAggr1.equals(postAggr0));assertTrue(postAggr0.equals(postAggr1));
		assertTrue(postAggr1.hashCode()==postAggr0.hashCode());
		assertTrue(!postAggr1.equals(new Object()));
	}

	
	public void testHyperUniqueCardinalityPostAggregator() {
		String postAggrName = "FieldAccessTest";

		String fieldName = "FieldName";
		HyperUniqueCardinalityPostAggregator postAggr = new HyperUniqueCardinalityPostAggregator(postAggrName, fieldName);

		byte[] cacheKey = postAggr.cacheKey();

		String hashCacheKeyExpected = "7019e13096d1bbe0f4fa7e725f2ccf33e016b117";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		System.out.println( DigestUtils.shaHex(cacheKey));
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		String fieldName1 = postAggr.getFieldName();
		assertEquals("FieldNames NOT Equals", fieldName, fieldName1);

		PostAggregatorType type = postAggr.getType();
		assertEquals("Type NOT Equals", PostAggregatorType.hyperUniqueCardinality, type);
		
		String postAggrName2 = "FieldAccessTest2";
		String fieldName2 = "FieldName2";
		HyperUniqueCardinalityPostAggregator postAggr0 = new HyperUniqueCardinalityPostAggregator(postAggrName, fieldName);
		assertTrue(postAggr0.equals(postAggr0));
		assertTrue(postAggr0.equals(postAggr));
		HyperUniqueCardinalityPostAggregator postAggr1 = new HyperUniqueCardinalityPostAggregator (null, null);
		assertTrue(!postAggr1.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr1));
		postAggr1 = new HyperUniqueCardinalityPostAggregator (postAggrName, null);
		assertTrue(!postAggr1.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr1));
		postAggr1 = new HyperUniqueCardinalityPostAggregator (postAggrName, fieldName2);
		assertTrue(!postAggr1.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr1));
		postAggr1 = new HyperUniqueCardinalityPostAggregator (postAggrName2, fieldName);
		assertTrue(!postAggr1.equals(postAggr0));assertTrue(!postAggr0.equals(postAggr1));
		
		postAggr1 = new HyperUniqueCardinalityPostAggregator (postAggrName, fieldName);
		assertTrue(postAggr1.equals(postAggr0));assertTrue(postAggr0.equals(postAggr1));
		assertTrue(postAggr1.hashCode()==postAggr0.hashCode());
		assertTrue(!postAggr1.equals(new Object()));
	}
	
	
	public void createAggregators() {

	}
}
