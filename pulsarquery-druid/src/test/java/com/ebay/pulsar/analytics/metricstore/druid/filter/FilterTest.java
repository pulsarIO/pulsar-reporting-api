/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.filter;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.FilterType;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.SelectorFilter;

public class FilterTest {
	@Test
	public void test() {
		testAndFilter();
		testNotFilter();
		testOrFilter();
		testRegexFilter();
		testSelectorFilter();
	}

	public void testAndFilter() {
		List<BaseFilter> fields = new ArrayList<BaseFilter> ();
		String dim1 = "Dimension1";
		String dim2 = "Dimension2";
		String val1 = "Value1";
		String val2 = "Value1";
		SelectorFilter filter1 = new SelectorFilter (dim1, val1);
		SelectorFilter filter2 = new SelectorFilter (dim2, val2);

		fields.add(filter1);
		fields.add(filter2);

		AndFilter andFilter = new AndFilter (fields);

		byte[] cacheKey = andFilter.cacheKey();

		String hashCacheKeyExpected = "7a7b8cb35c7169441f07a4d2dd306dcc570d03ba";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		List<BaseFilter> fieldZ = andFilter.getFields();
		SelectorFilter filter01 = (SelectorFilter) fieldZ.get(0);
		SelectorFilter filter02 = (SelectorFilter) fieldZ.get(1);
		assertEquals("Filter1 dimensions NOT Equals", filter1.getDimension(), filter01.getDimension());
		assertEquals("Filter1 values NOT Equals", filter1.getValue(), filter01.getValue());
		assertEquals("Filter2 dimensions NOT Equals", filter2.getDimension(), filter02.getDimension());
		assertEquals("Filter2 values NOT Equals", filter2.getValue(), filter02.getValue());
		FilterType type = andFilter.getType();
		assertEquals("AndFilter type NOT Equals", FilterType.and, type);

		// Nothing to do for FilterCacheHelper
		//FilterCacheHelper cacheHelper = new FilterCacheHelper();
		
		
		List<BaseFilter> fields2 = new ArrayList<BaseFilter> ();
		String dim21 = "Dimension1";
		String dim22 = "Dimension2";
		String val21 = "Value1";
		String val22 = "Value1";
		SelectorFilter filter21 = new SelectorFilter (dim21, val21);
		SelectorFilter filter22 = new SelectorFilter (dim22, val22);

		fields2.add(filter21);
		fields2.add(filter22);

		AndFilter filter_2 = new AndFilter (fields2);
		AndFilter filter_3 = new AndFilter (fields2);
		assertTrue(filter_2.equals(filter_2));
		assertTrue(filter_2.equals(filter_3));
		assertTrue(filter_2.hashCode()==filter_3.hashCode());
		assertTrue(!filter_2.equals(null));
		AndFilter filter_4 = new AndFilter (null);
		assertTrue(!filter_2.equals(filter_4));
		assertTrue(!filter_4.equals(filter_2));
	}

	public void testNotFilter() {
		String dim = "Dimension";
		String val = "Value";
		SelectorFilter filter = new SelectorFilter (dim, val);

		NotFilter notFilter = new NotFilter (filter);

		byte[] cacheKey = notFilter.cacheKey();

		String hashCacheKeyExpected = "d2144852978176ff6f1007d935ddc3904a7ee0b5";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		SelectorFilter filter1 = (SelectorFilter) notFilter.getField();
		filter1 = new SelectorFilter (dim, null);
		NotFilter notFilter1 = new NotFilter (filter1);

		byte[] cacheKey1 = notFilter1.cacheKey();

		String hashCacheKeyExpected1 = "d933af8a2771b093ad433eafa3b49b9754cc0e27";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey1);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);
		
		String dim2 = "Dimension";
		String val2 = "Value";
		SelectorFilter filter2 = new SelectorFilter (dim2, val2);

		NotFilter filter_2 = new NotFilter (filter2);
		NotFilter filter_3 = new NotFilter (filter2);
		assertTrue(filter_2.equals(filter_2));
		assertTrue(filter_2.equals(filter_3));
		assertTrue(filter_2.hashCode()==filter_3.hashCode());
		assertTrue(!filter_2.equals(null));
		NotFilter filter_4 = new NotFilter (null);
		assertTrue(!filter_2.equals(filter_4));
		assertTrue(!filter_4.equals(filter_2));
		
		assertTrue(!filter_2.equals(new NotFilter(filter2){
			
		}));
	}

	public void testOrFilter() {
		List<BaseFilter> fields = new ArrayList<BaseFilter> ();
		String dim1 = "Dimension1";
		String dim2 = "Dimension2";
		String val1 = "Value1";
		String val2 = "Value1";
		SelectorFilter filter1 = new SelectorFilter (dim1, val1);
		SelectorFilter filter2 = new SelectorFilter (dim2, val2);

		fields.add(filter1);
		fields.add(filter2);

		OrFilter orFilter = new OrFilter (fields);

		byte[] cacheKey = orFilter.cacheKey();

		String hashCacheKeyExpected = "2273255bed019827b6b1d5afbd0d0bdd3f2ea311";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);

		//List<BaseFilter> fields0 = orFilter.getFields();
		// Testing ONLY ONE field
		List<BaseFilter> fields1 = new ArrayList<BaseFilter> ();
		fields1.add(filter1);
		OrFilter orFilter1 = new OrFilter (fields1);
		cacheKey = orFilter1.cacheKey();
		String hashCacheKeyExpected1 = "557557f3afd3adbddb0d33dc0f50fd15e7ae6fcb";
		String hashCacheKeyGot1 = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected1, hashCacheKeyGot1);
		
		List<BaseFilter> fields2 = new ArrayList<BaseFilter> ();
		String dim21 = "Dimension1";
		String dim22 = "Dimension2";
		String val21 = "Value1";
		String val22 = "Value1";
		SelectorFilter filter21 = new SelectorFilter (dim21, val21);
		SelectorFilter filter22 = new SelectorFilter (dim22, val22);

		fields2.add(filter21);
		fields2.add(filter22);

		OrFilter filter_2 = new OrFilter (fields2);
		OrFilter filter_3 = new OrFilter (fields2);
		assertTrue(filter_2.equals(filter_2));
		assertTrue(filter_2.equals(filter_3));
		assertTrue(filter_2.hashCode()==filter_3.hashCode());
		assertTrue(!filter_2.equals(null));
		OrFilter filter_4 = new OrFilter (null);
		assertTrue(!filter_2.equals(filter_4));
		assertTrue(!filter_4.equals(filter_2));
	}

	public void testRegexFilter() {
		String dimension = "Dimension";
		String pattern = "A*B";
		RegexFilter filter = new RegexFilter (dimension, pattern);

		String dimension1 = filter.getDimension();
		String pattern1 = filter.getPattern();
		assertEquals("Dimension values NOT Equals", dimension, dimension1);
		assertEquals("Pattern values NOT Equals", pattern, pattern1);

		byte[] cacheKey = filter.cacheKey();

		String hashCacheKeyExpected = "7bbb96448f1f0755ad3fd9b5f5f8bee66f22ff98";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		
		String dimension2 = "Dimension2";
		String pattern2 = "A*B2";
		RegexFilter filter2 = new RegexFilter (dimension2, pattern2);
		RegexFilter filter1 = new RegexFilter (null,null);
		assertTrue(filter2.equals(filter2));
		assertTrue(!filter2.equals(filter1));
		assertTrue(!filter1.equals(filter2));
		filter1=new RegexFilter(dimension2,null);
		assertTrue(!filter2.equals(filter1));
		assertTrue(!filter1.equals(filter2));
		filter1=new RegexFilter(dimension2,pattern2);
		assertTrue(filter2.equals(filter1));
		assertTrue(filter2.hashCode()==filter1.hashCode());
		assertTrue(!filter2.equals(new  NotFilterEx(filter2){
			
		}));
		
		assertTrue(!filter2.equals(new  NotFilterEx(filter2){
			
		}));
		
	}
	
	public static class NotFilterEx extends NotFilter{

		public NotFilterEx(BaseFilter field) {
			super(field);
		}
		
	}
	
	public void testSelectorFilter() {
		String dim = "Dimension";
		String val = "Value";

		SelectorFilter filter = new SelectorFilter (dim, val);

		byte[] cacheKey = filter.cacheKey();

		String hashCacheKeyExpected = "1e3e9d10d888276e16f3565a8950e55b3b29a4ea";
		String hashCacheKeyGot = DigestUtils.shaHex(cacheKey);
		assertEquals("Hash of cacheKey NOT Equals", hashCacheKeyExpected, hashCacheKeyGot);
		
		String dim2 = "Dimension2";
		String val2 = "Value2";

		SelectorFilter filter2 = new SelectorFilter (dim2, val2);
		SelectorFilter filter1 = new SelectorFilter (null, null);
		
		assertTrue(filter2.equals(filter2));
		assertTrue(!filter2.equals(filter1));
		assertTrue(!filter1.equals(filter2));
		filter1=new SelectorFilter(dim2,null);
		assertTrue(!filter2.equals(filter1));
		assertTrue(!filter1.equals(filter2));
		filter1=new SelectorFilter(dim2,val2);
		assertTrue(filter2.equals(filter1));
		assertTrue(filter2.hashCode()==filter1.hashCode());
		assertTrue(!filter2.equals(new NotFilter(filter2){
			
		}));
	}

}
