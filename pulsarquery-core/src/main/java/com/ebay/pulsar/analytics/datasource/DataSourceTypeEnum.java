/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

/**
 * 
 * @author mingmwang
 *
 */
public enum DataSourceTypeEnum {
	PULSAR("pulsar"), DRUID("druid");

	private final String type;

	private DataSourceTypeEnum(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}

	public static DataSourceTypeEnum fromType(String typeStr) {
		if (typeStr != null) {
			for (DataSourceTypeEnum typeEnum : DataSourceTypeEnum.values()) {
				if (typeEnum.getType().equalsIgnoreCase(typeStr)) {
					return typeEnum;
				}
			}
		}
		return null;
	}
}
