/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.datasource;

import java.lang.reflect.Method;
import java.util.EnumMap;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

/**
 * 
 * @author mingmwang
 *
 */
public class DataSourceTypeRegistry {
	private static EnumMap<DataSourceTypeEnum, Class<? extends DataSourceProviderFactory>> factryRegistry = Maps.newEnumMap(DataSourceTypeEnum.class);
	
	public static void registerDataSourceType(DataSourceTypeEnum dataSourceType, Class<? extends DataSourceProviderFactory> clazz){
		factryRegistry.put(dataSourceType, clazz);
	}
	
	public static DataSourceProviderFactory getDataSourceFactory(DataSourceTypeEnum dataSourceType){
		Class<? extends DataSourceProviderFactory> clazz = factryRegistry.get(dataSourceType);
		if(clazz == null)
			return null;
		try {
			Method getInstance = clazz.getMethod("getInstance", new Class<?>[0]);
			return (DataSourceProviderFactory)getInstance.invoke(null, new Object[0]);
		} catch (Exception e) {
			return null;
		} 
	}
	
	public static Set<String> getAllSupportedDataSourceTypes(){
		return FluentIterable.from(factryRegistry.keySet())
				.transform(new Function<DataSourceTypeEnum, String>() {
					@Override
					public String apply(DataSourceTypeEnum input) {
						if(input!=null)
							return input.getType();
						return null;
					}
				}).toSet();
	}
}
