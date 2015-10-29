/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao.mapper;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.NumberUtils;

public class BaseDBMapper<T> implements RowMapper<T> {
	private static final Logger logger = LoggerFactory.getLogger(BaseDBMapper.class);
	Class<T> clazz=null;
	@SuppressWarnings("unchecked")
	public BaseDBMapper(){
		ParameterizedType pt=(ParameterizedType)this.getClass().getGenericSuperclass();
		clazz=(Class<T>)pt.getActualTypeArguments()[0];
	}
	/**
	 * 
	 * @return
	 */
	public Class<T> clazz(){
		return this.clazz;
		
	}
	@SuppressWarnings("unchecked")
	@Override
	public T mapRow(ResultSet r,int index) throws SQLException {
		try {
			T obj=(T)clazz.newInstance();
			if(obj == null){  
	            return null;  
	        }          
	        try {  
	        	
	            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);  
	            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();  
	            for (PropertyDescriptor property : propertyDescriptors) {  
	                String key = property.getName();  
	                if (!key.equals("class")) {  
	                	Object value=null;
	                	try{
	                    Method setter = property.getWriteMethod(); 
	                    value=r.getObject(key.toLowerCase());
	                    if(value!=null && value instanceof Number){
							@SuppressWarnings("rawtypes")
							Class[] types=setter.getParameterTypes();
	                    	value=NumberUtils.convertNumberToTargetClass((Number)value, types[0]);
	                    }
	                    if(value!=null){
	                    	if(value.getClass().equals(BigInteger.class)){
	                    		setter.invoke(obj, ((BigInteger)value).longValue());
	                    	}else if(value.getClass().equals(byte[].class)){
	                    		setter.invoke(obj, new String((byte[])value));
	                    	}else if(Blob.class.isAssignableFrom(value.getClass())){
	                    		Blob bv=(Blob)value;
	                    		 byte[] b = new byte[(int)bv.length()];
	                    		 InputStream stream = bv.getBinaryStream();
	                    		 stream.read(b);
	                    		 stream.close();
	                    		String v=new String(b);
	                    		setter.invoke(obj, v);
	                    	}else{
	                    		setter.invoke(obj, value);
	                    	}
	                    }
	                	}catch(Exception e){
	                		logger.error("transBean2Map Error " + e);
	                		logger.error("name["+key+"]="+(value==null?"NULL":value.toString())+", class:"+(value==null?"NULL":value.getClass())+", err:"+e.getMessage());
	                	}
	                    
	                }  
	  
	            }  
	        } catch (Exception e) {  
	            logger.error("transBean2Map Error " + e);  
	        }  
	        return obj;
		} catch (Exception e) {
			logger.error("Exception:"+e);
		} 
	
		return null;
	}

	
}
