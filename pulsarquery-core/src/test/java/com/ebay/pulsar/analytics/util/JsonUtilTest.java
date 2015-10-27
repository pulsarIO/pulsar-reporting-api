/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.util;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

public class JsonUtilTest {

	@Test
	public void test() throws JsonProcessingException {
		Map<String,String> map=new HashMap<String,String>();
		map.put("testKay", "testValue");
 
		//JsonUtil.readValue(src, valueType);
		assertEquals("{\"testKay\":\"testValue\"}",JsonUtil.writeValueAsString(map));
		assertEquals("{\r\n  \"testKay\" : \"testValue\"\r\n}",JsonUtil.writeValueAsIndentString(map));
		assertEquals("{\"testKay\":\"testValue\"}".getBytes().length,JsonUtil.writeValueAsBytes(map).length);
	}
}
