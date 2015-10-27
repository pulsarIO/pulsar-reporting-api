/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao.service;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.sql.rowset.serial.SerialBlob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import com.ebay.pulsar.analytics.dao.DBFactory;
import com.ebay.pulsar.analytics.dao.DBService;
import com.ebay.pulsar.analytics.dao.RDBMS;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class BaseDBService<T> implements DBService<T> {
	private static final Logger logger = LoggerFactory.getLogger(BaseDBService.class);
	public static final String QUTOA="";
	protected RDBMS db;
	public BaseDBService(){
		//db=DBFactory.instance();
	}
	public void setDb(RDBMS db){
		this.db=db;
	}
	public RDBMS getDB(){
		if(db==null) db=DBFactory.instance();
		return db;
	}
	
	
	public abstract String getTableName();
	public abstract RowMapper<T> mapper();
	
	public String getTablePrefix(){
		return DBFactory.getPrefix();
	}
	
	@Override
	public T getById(long id) {
		return getDB().get("select * from "+QUTOA+getTableName()+QUTOA+" where id=:id", 
				ImmutableMap.of("id", id), mapper());
	}
	@Override
	public List<T> getAll(){
		return getDB().query("select * from "+QUTOA+getTableName()+QUTOA,ImmutableMap.<String,Object>of(),-1, mapper());
	}
	@Override
	public List<T> get(T condition) {
		return get(condition,-1);
	}
	
	public List<T> getAllByColumnIn(String column, List<?> in, int maxSize){
		Set<?> unique=Sets.newHashSet(in);
		if(unique.size()==0) return Lists.newArrayList();
		String sql="select * from "+QUTOA+getTableName()+QUTOA+" where "+QUTOA+column+QUTOA+" in (:INPARAMETER)";
		MapSqlParameterSource parameters = new MapSqlParameterSource();
		parameters.addValue("INPARAMETER", unique);
		return this.getDB().query(sql, parameters, maxSize, mapper());
	}
	
	public List<T> get(T condition,int maxRows) {
		Object obj=condition;
		Map<String, Object> p=describe(obj);
		
		Map<String,Object> prop=new HashMap<String,Object>();
		StringBuilder whereCluse=new StringBuilder();
		if(p!=null)
		for(Entry<String,Object> entry: p.entrySet()){
			if(entry.getValue()!=null){
				String newKey=entry.getKey().toLowerCase();
				prop.put(newKey, entry.getValue());
				whereCluse.append(QUTOA).append(newKey).append(QUTOA).append("=:").append(newKey).append(" and ");
			}
		}
		String where=null;
		if(whereCluse.length()>0){
			where=" where "+whereCluse.substring(0, whereCluse.length()-" and ".length());
		}
		if(logger.isDebugEnabled()){
			logger.info("select:"+"select * from "+getTableName()+(where==null?"":where));
		}
		return getDB().query("select * from "+QUTOA+getTableName()+QUTOA+(where==null?"":where), prop, maxRows, mapper());
	}
	@Override
	public int updateById(T update) {
		Object obj=update;
		Map<String, Object> p=describe(obj);
		Map<String,Object> prop=Maps.newHashMap();
		StringBuilder set=new StringBuilder();
		String where=null;
		if(p!=null){
			for(Entry<String,Object> entry: p.entrySet()){
				if(entry.getValue()!=null && !"id".equalsIgnoreCase(entry.getKey())){
					String newKey=entry.getKey().toLowerCase();
					set.append(QUTOA).append(newKey).append(QUTOA).append("=:"+newKey).append(",");
					prop.put(newKey, entry.getValue());
				}
			}
			if(p.containsKey("id")){
				where=" where id=:id";
				prop.put("id", p.get("id"));
			}
		}
		String setStr=null;
		if(set.length()>0){
			setStr="set "+set.substring(0, set.length()-1);
		}
		if(logger.isDebugEnabled()){
			logger.info("update "+QUTOA+getTableName()+QUTOA+" "+setStr+where);
		}
		return getDB().update("update "+QUTOA+getTableName()+QUTOA+" "+setStr+where, prop);
	}
	public static Map<String, Object> describe(Object obj) {  
        if(obj == null){  
            return null;  
        }          
        Map<String, Object> map = new HashMap<String, Object>();  
        try {  
            BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());  
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();  
            for (PropertyDescriptor property : propertyDescriptors) {  
                String key = property.getName();  
                if (!key.equals("class")) {  
                	try{
                    Method getter = property.getReadMethod();  
                    Object value = getter.invoke(obj);  
                    if(value!= null){
                    	
                    	if("properties".equalsIgnoreCase(key) || "config".equalsIgnoreCase(key)){
                    		value=new SerialBlob(((String)value).getBytes());
            			}
                    	map.put(key, value);
                    }
                	}catch(Exception e){
                		logger.error("transBean1Map Error " + e); 
                	}
                }  
  
            }  
        } catch (Exception e) {  
            logger.error("transBean2Map Error " + e);  
        }  
        return map;  
  
    }  
	@Override
	public long inser(T insert) {
		Object obj=insert;
		Map<String, Object> p=describe(obj);
		//Object[] prop=new Object[p.size()];
		Map<String,Object> prop=Maps.newHashMap();
		StringBuilder columnClause=new StringBuilder();
		StringBuilder valueClause=new StringBuilder();
		if(p!=null){
			for(Entry<String,Object> entry: p.entrySet()){
			if(entry.getValue()!=null && !"class".equalsIgnoreCase(entry.getKey())){
				String newKey=entry.getKey().toLowerCase();
				columnClause.append(QUTOA).append(newKey).append(QUTOA).append(",");
				valueClause.append(":").append(newKey).append(",");
				prop.put(newKey,entry.getValue());
				//prop[index++]=entry.getValue();
			}
		}
		}
		String column=null;
		String value=null;
		if(columnClause.length()>0){
			column="("+columnClause.substring(0, columnClause.length()-1)+")";
			value="("+valueClause.substring(0, valueClause.length()-1)+")";
		}
		if(logger.isDebugEnabled()){
			logger.info("insert into "+QUTOA+getTableName()+QUTOA+" "+column+" values "+value);
		}
		KeyHolder keyHolder = new GeneratedKeyHolder();
		int row= getDB().insert("insert into "+QUTOA+getTableName()+QUTOA+" "+column+" values "+value, prop, keyHolder);
		if(row>0) return keyHolder.getKey().longValue();
		return -1;
	}
	@Override
	public int deleteById(long id){
		if(logger.isDebugEnabled()){
			logger.info("delete from "+QUTOA+getTableName()+QUTOA+" where "+QUTOA+"id"+QUTOA+"=:id");
		}
		return getDB().delete("delete from "+QUTOA+getTableName()+QUTOA+" where "+QUTOA+"id"+QUTOA+"=:id", ImmutableMap.of("id",id));
	}
	@Override
	public int deleteBatch(T condition){
		Object obj=condition;
		Map<String, Object> p=describe(obj);
		Map<String,Object> prop=new HashMap<String,Object>();
		StringBuilder whereCluse=new StringBuilder();
		if(p!=null)
		for(Entry<String,Object> entry: p.entrySet()){
			if(entry.getValue()!=null){
				String newKey=entry.getKey().toLowerCase();
				prop.put(newKey, entry.getValue());
				whereCluse.append(QUTOA).append(newKey).append(QUTOA).append("=:").append(newKey).append(" and ");
			}
		}
		String where=null;
		if(whereCluse.length()>0){
			where=" where "+whereCluse.substring(0, whereCluse.length()-" and ".length());
		}
		if(logger.isDebugEnabled()){
			logger.info("delete:"+"delete  from "+getTableName()+(where==null?"":where));
		}
		return getDB().delete("delete  from "+QUTOA+getTableName()+QUTOA+(where==null?"":where), prop);
	}
	public int execute(String sql,Map<String,?> param){
		return getDB().update(sql,param);
	}
}
