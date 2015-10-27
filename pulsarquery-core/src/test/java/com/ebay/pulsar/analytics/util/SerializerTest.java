/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.util;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.ebay.pulsar.analytics.dao.model.DBDataSource;

public class SerializerTest {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSerializer() throws IOException{
		Serializer serializer=new Serializer(DBDataSource.class);
		DBDataSource datasource=new DBDataSource();
		datasource.setName("test");
		assertTrue(serializer.deserialize(serializer.serialize(datasource)).equals(datasource));
	}

}
