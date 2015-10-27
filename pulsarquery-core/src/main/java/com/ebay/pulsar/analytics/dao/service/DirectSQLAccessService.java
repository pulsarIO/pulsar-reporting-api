/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao.service;

import java.util.List;

import com.ebay.pulsar.analytics.dao.DBFactory;
import com.ebay.pulsar.analytics.dao.RDBMS;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

/**
 *@author qxing
 * 
 **/
public class DirectSQLAccessService {
	protected RDBMS db;
	
	public DirectSQLAccessService(){
	}
	public void setDb(RDBMS db){
		this.db=db;
	}
	public RDBMS getDB(){
		if(db==null) db=DBFactory.instance();
		return db;
	}
	
	public List<String> getAllRightsForValidUser(String userName){
		String sql="select rightname as name from "+DBFactory.getPrefix()+"DBRightGroup where groupname in ('public')";
		String sql2="select rightname as name from "+DBFactory.getPrefix()+"DBRightGroup where groupname in (\n"
				+" 	select groupname from "+DBFactory.getPrefix()+"DBUserGroup where username = :userName\n"
				+" 	union\n"
				+" 	select name as groupname from "+DBFactory.getPrefix()+"DBGroup where owner = :userName\n"
				+")\n";
		String sql3 ="select name from "+DBFactory.getPrefix()+"DBDashboard where owner = :userName\n"
				+"union\n"
				+"select name from "+DBFactory.getPrefix()+"DBDatasource where owner = :userName\n"
				+"union\n"
				+"select name from "+DBFactory.getPrefix()+"DBGroup where owner = :userName\n";
		List<String> rights1= getDB().queryForList(sql, ImmutableMap.of("userName",userName), -1);
		List<String> rights2=getDB().queryForList(sql2, ImmutableMap.of("userName",userName), -1);
		List<String> rights3=getDB().queryForList(sql3, ImmutableMap.of("userName",userName), -1);
		return FluentIterable.from(rights3).transform( new Function<String,String>(){
			@Override
			public String apply(String input) {
				return input+"_MANAGEG";
			}
		}).append(rights1).append(rights2).toList();
		
		
	}
	
	public List<String> getAllRightsForValidUser(String userName, int maxRows){
		
		String sql ="select name||'_MANAGE' from "+DBFactory.getPrefix()+"DBDashboard where owner = :userName\n"
				+"union\n"
				+"select name||'_MANAGE' from "+DBFactory.getPrefix()+"DBDatasource where owner = :userName\n"
				+"union\n"
				+"select name||'_MANAGE' from "+DBFactory.getPrefix()+"DBGroup where owner = :userName\n"
				+"union\n"
				+"select rightname as name from "+DBFactory.getPrefix()+"DBRightGroup where groupname in (\n"
				+" 	select groupname from "+DBFactory.getPrefix()+"DBUserGroup where username = :userName\n"
				+" 	union\n"
				+" 	select name as groupname from "+DBFactory.getPrefix()+"DBGroup where owner = :userName\n"
				+" 	union\n"
				+" 		VALUES('public')\n"
				+")\n";
//		String sql="select CONCAT(name,'_MANAGE') from DBDashboard A, DBUserGroup B where A.owner =:userName or (B.userName =:userName and B.groupName='admin')\n" 
//				+"union\n"
//				+"select CONCAT(name,'_MANAGE') from DBDatasource A, DBUserGroup B where owner =:userName or (B.userName =:userName and B.groupName='admin')\n"
//				+"union\n"
//				+"select CONCAT(name,'_MANAGE') from DBGroup A, DBUserGroup B where owner =:userName or (B.userName =:userName and B.groupName='admin')\n"
//				+"union\n"
//				+"select rightname as name from DBRightGroup where groupname in (\n"
//				+" 	select groupname from DBUserGroup where username =:userName\n"
//				+" 	union\n"
//				+"		select name as groupname from DBGroup where owner =:userName\n"
//				+" 	union\n"
//				+" 		select 'public' as groupname\n"
//				+")\n";
		return getDB().queryForList(sql, ImmutableMap.of("userName",userName), maxRows);
	}
}
