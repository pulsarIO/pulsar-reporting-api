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

public class SlugifyTest {
	private Slugify slg = new Slugify();

	@Test
	public void testSlugify() {
		slg.setLowerCase(false);
		assertEquals("Hello_World", slg.slugify("Hello   World"));
		assertEquals("HelloWorld", slg.slugify("HelloWorld"));
		slg.setLowerCase(true);
		assertEquals("hello_world", slg.slugify("Hello World"));
		Map<String, String> customReplacements = new HashMap<String, String>();
		customReplacements.put("Hello","x");
		slg.setCustomReplacements(customReplacements);
		assertEquals("x_world", slg.slugify("Hello World") );
	}

}
