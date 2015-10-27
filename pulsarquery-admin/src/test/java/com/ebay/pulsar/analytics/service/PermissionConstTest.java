/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;

import org.junit.Assert;
import org.junit.Test;

public class PermissionConstTest {
	
    @Test
    public void testUtils() throws Exception{
    	//Mock to improver ut coverage
    	Assert.assertEquals("public", PermissionConst.PUBLICGROUP);
    	Assert.assertEquals("%s_MANAGE", PermissionConst.MANAGE_RIGHT_TEMPLATE);
    	Assert.assertEquals("%s_VIEW", PermissionConst.VIEW_RIGHT_TEMPLATE);
    	Assert.assertEquals("%s_%s", PermissionConst.DATA_TABLE_RIGHT_TEMPLATE);
    	Assert.assertEquals("%s_%d", PermissionConst.RESOURCE_NAME_TEMPLATGE);
    	Assert.assertEquals(0, PermissionConst.RIGHT_TYPE_SYS);
    	Assert.assertEquals(1, PermissionConst.RIGHT_TYPE_DATA);
    	Assert.assertEquals(2, PermissionConst.RIGHT_TYPE_DASHBOARD);
    	Assert.assertEquals(3, PermissionConst.RIGHT_TYPE_MENU);
    	Assert.assertEquals(4, PermissionConst.RIGHT_TYPE_GROUP);
    }
    
    
}
