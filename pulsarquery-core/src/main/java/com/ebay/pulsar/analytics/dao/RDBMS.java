/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.AbstractSqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

import com.google.common.collect.ImmutableMap;

public class RDBMS {

    public static final String JDBC_DATABASE_BEAN_NAME = "jdbcDatabase";
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private String userName;
    private String userPwd;
    private String url;
    private String driver;
    private boolean testOnBorrow=true;
    private String validationQuery=null;
    private DataSource dataSource=null;
    
    public void setDataSource(DataSource dataSource) {
    	this.dataSource=dataSource;
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(this.dataSource);
    }

    public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
        return namedParameterJdbcTemplate;
    }

    public void setNamedParameterJdbcTemplate(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    public Boolean execute(final String sql,final Map<String,?> parameters) {
        return this.namedParameterJdbcTemplate.execute(sql,parameters, new PreparedStatementCallback<Boolean>() {
            @Override
            public Boolean doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
//            	if(ps.getParameterMetaData()!=null){
//            		int count=ps.getParameterMetaData().getParameterCount();
//            		for(int i=0;i<count;i++){
//            			String name=ps.getParameterMetaData().getParameterTypeName(i);
//            			System.out.println(i+":"+name);
//            			if(name.equalsIgnoreCase("properties") || "config".equalsIgnoreCase(name)){
//            				Blob bv=ps.getConnection().createBlob();
//            				bv.setBytes(0, ((String)parameters.get(name)).getBytes());
//            				ps.setBlob(i, bv);
//            			}
//            		}
//            		return ps.execute();
//            	}else{
//            		return ps.execute();
//            	}
            	return ps.execute();
            }
        });
    }
    public int queryForInt(final String sql, Map<String,?> parameters){
    	return this.namedParameterJdbcTemplate.queryForObject(sql, new MapSqlParameterSource(parameters) , Integer.class);
    }
    public void execute(String sql){
    	execute(sql,ImmutableMap.<String,Object>of());
    }
    public <T> T get(String query, Map<String,?> parameters, final RowMapper<T> mapper){
    	return this.namedParameterJdbcTemplate.query(query, parameters, new ResultSetExtractor<T>(){
			@Override
			public T extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				if(rs.next()){
					return mapper.mapRow(rs, 0);
				}
				return null;
			}
    		
    	});
    }
    public <T> List<T> queryForList(String sql,Map<String,?> parameters, final int maxrows){
    	final RowMapper<T> mapper=new SingleColumnRowMapper<T>();
    	return this.namedParameterJdbcTemplate.query(sql, parameters, new ResultSetExtractor<List<T>>(){
			@Override
			public List<T> extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				List<T> data = new ArrayList<T>();
		        if(rs==null) return data;
		        int rowNum = 0;
		        while((maxrows>0 && rowNum<maxrows || maxrows<0) && rs.next()){
		        	data.add(mapper.mapRow(rs, rowNum++));
		        }
		        return data;
			}
    	});
    }
    public <T> List<T> query(String sql, Map<String,?> parameters,final int maxrows,final RowMapper<T> mapper){
    	return this.namedParameterJdbcTemplate.query(sql, parameters, new ResultSetExtractor<List<T>>(){
			@Override
			public List<T> extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				List<T> data = new ArrayList<T>();
		        if(rs==null) return data;
		        int rowNum = 0;
		        while((maxrows>0 && rowNum<maxrows || maxrows<0) && rs.next()){
		        	data.add(mapper.mapRow(rs, rowNum++));
		        }
		        return data;
			}
    	});
    }
    public <T> List<T> query(String sql, AbstractSqlParameterSource parameters,final int maxrows,final RowMapper<T> mapper){
    	return this.namedParameterJdbcTemplate.query(sql, parameters, new ResultSetExtractor<List<T>>(){
			@Override
			public List<T> extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				List<T> data = new ArrayList<T>();
		        if(rs==null) return data;
		        int rowNum = 0;
		        while((maxrows>0 && rowNum<maxrows || maxrows<0) && rs.next()){
		        	data.add(mapper.mapRow(rs, rowNum++));
		        }
		        return data;
			}
    	});
    }
    public int update(String sql, Map<String,?> parameters){
    	return this.namedParameterJdbcTemplate.update(sql, parameters);
    }
   
    public int insert(final String sql, Map<String,?> parameter, KeyHolder keyHolder){
    	 SqlParameterSource sqlParameter = new MapSqlParameterSource(parameter);
    	 return this.namedParameterJdbcTemplate.update(sql, sqlParameter, keyHolder);
    }
    public int delete(String sql, Map<String,?> parameter){
    	return this.namedParameterJdbcTemplate.update(sql, parameter);
    }
     /**
      * 
      * @param timeout  timeout for test in second
      * @return
      */
     public boolean testConnection(int timeout){
     	return true;
     }
//     public boolean tableExists(String tableName){
//    	 //List<String> tables= this.namedParameterJdbcTemplate.queryForList("show tables;", ImmutableMap.of("tableName",tableName), String.class);
//    	 List<Map<String,Object>> tables=this.namedParameterJdbcTemplate.queryForList("show tables",ImmutableMap.of("tableName",tableName));
//    	 return tables!=null && tables.size()>0;
//     }
     public void createTable(String tableName, String sql){
    	 //if(!tableExists(tableName)){
    		this.execute(sql);
    	 //}
     }
    
     
     public RDBMS(String driver, String url, String userName, String userPwd) {
         this.userName = userName;
         this.userPwd = userPwd;
         this.url = url;
         this.driver = driver;
         this.init(true);
     }
     public RDBMS(DataSource dataSource){
    	this.setDataSource(dataSource); 
     }
     public void close(){
    	 try {
			((BasicDataSource)dataSource).close();
		} catch (SQLException e) {
		}
     }
     
     private boolean init(boolean force) {
         if (dataSource == null || force) {
         	final BasicDataSource dataSource = new BasicDataSource();
 	        dataSource.setUsername(userName);
 	        dataSource.setPassword(userPwd);
 	        dataSource.setUrl(url);
 	        dataSource.setTestOnBorrow(true);
 	        if(validationQuery!=null)
 	        	dataSource.setValidationQuery(validationQuery);
 	        dataSource.setDriverClassLoader(Thread.currentThread().getContextClassLoader());
 	        dataSource.setDriverClassName(driver);
 	        this.setDataSource(dataSource);
         }
         return true;
     }

     public String getUserName() {
         return userName;
     }

     public void setUserName(String userName) {
         this.userName = userName;
     }

     public String getUserPwd() {
         return userPwd;
     }

     public void setUserPwd(String userPwd) {
         this.userPwd = userPwd;
     }

     public String getUrl() {
         return url;
     }

     public void setUrl(String url) {
         this.url = url;
     }

     public String getDriver() {
         return driver;
     }

     public void setDriver(String driver) {
         this.driver = driver;
     }

 	public boolean isTestOnBorrow() {
 		return testOnBorrow;
 	}

 	public void setTestOnBorrow(boolean testOnBorrow) {
 		this.testOnBorrow = testOnBorrow;
 	}

 	public String getValidationQuery() {
 		return validationQuery;
 	}

 	public void setValidationQuery(String validationQuery) {
 		this.validationQuery = validationQuery;
 	}
    
}
