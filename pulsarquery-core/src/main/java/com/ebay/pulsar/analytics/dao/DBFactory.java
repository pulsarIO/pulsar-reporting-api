/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.dao;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * Database Connection and Initialization.
 * 
 *@author qxing
 * 
 **/
public class DBFactory {
	private static volatile DataSource ds;
	private static volatile String prefix;
	private static volatile RDBMS db;
	
	public static RDBMS instance() {
		if(db==null){
			synchronized(DBFactory.class){
				if(db==null){
					if(ds!=null){
						db = new RDBMS(ds);
						createDatabase(db);
					}
				}
			}
		}
		return db;
		//return RDBMSConfigHolder.db;
	}

	private DBFactory() {
	};

//	private static class RDBMSConfigHolder {
//		private static RDBMS db;
//		static {
//			db = new RDBMS(ds);
//			createDatabase(db);
//		}
//	}
	public static void setPrefix(String prefix){
		DBFactory.prefix=prefix;
		System.out.println("table.prefix="+prefix);
	}
	public static String getPrefix(){
		return prefix;
	}
	public static void setDs(BasicDataSource datasource) {
		//
		// First, we'll create a ConnectionFactory that the
		// pool will use to create Connections.
		// We'll use the DriverManagerConnectionFactory,
		// using the connect string passed in the command line
		// arguments.
		//
		try {
			Class.forName(datasource.getDriverClassName());
		} catch (ClassNotFoundException e) {
		}
		ConnectionFactory connectionFactory =
			new DriverManagerConnectionFactory(datasource.getUrl(), datasource.getUsername(), datasource.getPassword());
		
		//
		// Next we'll create the PoolableConnectionFactory, which wraps
		// the "real" Connections created by the ConnectionFactory with
		// the classes that implement the pooling functionality.
		//
		PoolableConnectionFactory poolableConnectionFactory =
			new PoolableConnectionFactory(connectionFactory, null);
		
		//
		// Now we'll need a ObjectPool that serves as the
		// actual pool of connections.
		//
		// We'll use a GenericObjectPool instance, although
		// any ObjectPool implementation will suffice.
		//
		ObjectPool<PoolableConnection> connectionPool =
				new GenericObjectPool<>(poolableConnectionFactory);
		
		// Set the factory's pool property to the owning pool
		poolableConnectionFactory.setPool(connectionPool);
		
		//
		// Finally, we create the PoolingDriver itself,
		// passing in the object pool we created.
		//
		PoolingDataSource<PoolableConnection> poolingDS =
				new PoolingDataSource<>(connectionPool);
		ds = poolingDS;
	}

	public static String[] DBWhiteList = { "admin"};
	

	private static void createDatabase(RDBMS db) {
		createDashboard(db, getPrefix()+"DBDashboard");
		createDataSource(db, getPrefix()+"DBDatasource");
		createGroup(db, getPrefix()+"DBGroup");
		//createMenu(db, getPrefix()+"DBMenu");
		createTables(db, getPrefix()+"DBTables");
		createUser(db, getPrefix()+"DBUser");
		createRightGroup(db, getPrefix()+"DBRightGroup");
		createUserGroup(db, getPrefix()+"DBUserGroup");

	}
	
	
	private static void createDashboard(RDBMS db, String tableName) {
		String table="CREATE TABLE %1$s (\n"
				+"id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
				+"name VARCHAR(100) NOT NULL,\n"
				+"owner VARCHAR(64) NOT NULL,\n"
				+"config BLOB,\n"
				+"displayname VARCHAR(64),\n"
				+"createtime TIMESTAMP,\n"
				+"lastupdatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
//				+"PRIMARY KEY (id),\n"
//				+"UNIQUE INDEX id (id),\n"
//				+"UNIQUE INDEX name (name),\n"
//				+"INDEX owner (owner)\n"
				+")\n";
		
		//if (!db.tableExists(tableName)) {
			db.createTable(tableName, String.format(table, tableName));
		//}
	}

	private static void createDataSource(RDBMS db, String tableName) {
		String table = "CREATE TABLE %1$s (\n"
				+ "id BIGINT  NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
				+ "name VARCHAR(100) NOT NULL,\n"
				+ "type VARCHAR(255),\n"
				+ "displayname VARCHAR(64),\n"
				+ "owner VARCHAR(64) NOT NULL,\n"
				+ "endpoint VARCHAR(1028) NOT NULL,\n"
				+ "properties BLOB,\n"
				+ "comment VARCHAR(100),\n"
				+ "createtime TIMESTAMP,\n"
				+ "lastupdatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
				+ "readonly boolean NOT NULL DEFAULT true\n"
//				+ "PRIMARY KEY (id),\n"
//				+ "UNIQUE INDEX id (id),\n"
//				+ "UNIQUE INDEX name (name),\n"
//				+ "INDEX owner (owner)\n"
			+ ")\n";
		
		//if (!db.tableExists(tableName)) {
			db.createTable(tableName, String.format(table, tableName));
		//}
	}

	private static void createGroup(RDBMS db, String tableName) {
		String table = "CREATE TABLE %1$s (\n"
				+ "id BIGINT  NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
				+ "name VARCHAR(100) NOT NULL,\n"
				+ "owner VARCHAR(64) NOT NULL DEFAULT '0',\n"
				+ "comment VARCHAR(256) DEFAULT '0',\n"
				+ "displayname VARCHAR(64),\n"
				+ "createTime TIMESTAMP,\n"
				+ "lastupdatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
//				+ "PRIMARY KEY (id),\n"
//				+ "UNIQUE INDEX name (name),\n"
//				+ "INDEX owner (owner)\n"
			+ ")\n";
		
		//if (!db.tableExists(tableName)) {
			db.createTable(tableName, String.format(table, tableName));
			db.execute("insert into "+getPrefix()+"DBGroup(name,owner,displayname) values('public','admin','public')");
			db.execute("insert into "+getPrefix()+"DBGroup(name,owner,displayname) values('admin','admin','admin')");
		//}
	}

//	private static void createMenu(RDBMS db, String tableName) {
//		String table = "CREATE TABLE %1$s (\n"
//				+ "id BIGINT  NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
//				+ "name VARCHAR(100) NOT NULL,\n"
//				+ "displayName VARCHAR(64) NOT NULL DEFAULT '0',\n"
//				+ "owner VARCHAR(64) NOT NULL DEFAULT '0',\n"
//				+ "level SMALLINT NOT NULL DEFAULT '0',\n"
//				+ "parent BIGINT NOT NULL DEFAULT -,\n"
//				+ "createTime TIMESTAMP,\n"
//				+ "lastupdatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
////				+ "PRIMARY KEY (id),\n"
////				+ "UNIQUE INDEX name (name),\n"
////				+ "INDEX owner (owner)\n"
//			+ ")\n";
//		
//		//if (!db.tableExists(tableName)) {
//			db.createTable(tableName, String.format(table, tableName));
//		//}
//	}

	private static void createRightGroup(RDBMS db, String tableName) {
		String table = "CREATE TABLE %1$s (\n"
				+ "id BIGINT  NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
				+ "groupName VARCHAR(100) NOT NULL,\n"
				+ "rightType INTEGER NOT NULL DEFAULT 0,\n"
				+ "rightName VARCHAR(128) NOT NULL,\n"
				+ "createTime TIMESTAMP,\n"
				+ "lastupdatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
//				+ "PRIMARY KEY (id),\n"
//				+ "UNIQUE INDEX groupName_rightType_rightName (groupName, rightType, rightName),\n"
//				+ "INDEX groupName (groupName),\n"
//				+ "CONSTRAINT FK_"+getPrefix()+"DBRightGroup_DBGroup FOREIGN KEY (groupName) REFERENCES "+getPrefix()+"DBGroup (name)\n"
			+ ")\n";

		//if (!db.tableExists(tableName)) {
			db.createTable(tableName, String.format(table, tableName));
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_MANAGE_GROUP')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_MANAGE_DATASOURCE')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_MANAGE_DASHBOARD')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_VIEW_GROUP')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_VIEW_DATASOURCE')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_VIEW_DASHBOARD')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('admin',0,'ADD_GROUP')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('admin',0,'ADD_DATASOURCE')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('admin',0,'ADD_DASHBOARD')");	
			
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('public',0,'ADD_DATASOURCE')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('public',0,'ADD_DASHBOARD')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('public',1,'tracking_VIEW')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('public',1,'trackingdruid_VIEW')");
			db.execute("insert into "+getPrefix()+"DBRightGroup(groupName,rightType,rightName) values('public',1,'pulsarholap_VIEW')");
			
		//}
	}

	private static void createTables(RDBMS db, String tableName) {
		String table = "CREATE TABLE %1$s (\n"
				+ "id BIGINT  NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
				+ "name VARCHAR(32) NOT NULL,\n"
				+ "datasourcename VARCHAR(100) NOT NULL,\n"
				+ "columns BLOB,\n"
				+ "comment VARCHAR(50) DEFAULT '0',\n"
				+ "createtime TIMESTAMP,\n"
				+ "lastupdatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
//				+ "PRIMARY KEY (id),\n"
//				+ "INDEX FK_"+getPrefix()+"DBTables_DBDatasource (datasourcename),\n"
//				+ "CONSTRAINT FK_"+getPrefix()+"DBTables_DBDatasource FOREIGN KEY (datasourcename) REFERENCES "+getPrefix()+"DBDatasource (name)\n"
			+ ")\n";
		//if (!db.tableExists(tableName)) {
			db.createTable(tableName, String.format(table, tableName));
		//}
	}

	private static void createUser(RDBMS db, String tableName) {
		String table = "CREATE TABLE %1$s (\n"
				+ "id BIGINT  NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
				+ "name VARCHAR(64) NOT NULL,\n"
				+ "comment VARCHAR(128),\n"
				+ "password VARCHAR(255) NOT NULL,\n"
				+ "email VARCHAR(255),\n"
				+ "image VARCHAR(255),\n"
				+ "enabled boolean DEFAULT true,\n"
				+ "createTime TIMESTAMP,\n"
				+ "lastupdatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
//				+ "PRIMARY KEY (id),\n"
//				+ "UNIQUE INDEX id (id),\n"
//				+ "UNIQUE INDEX name (name)\n"
			+ ")\n";
		
		//if (!db.tableExists(tableName)) {
			db.createTable(tableName, String.format(table, tableName));
			
			for(String name : DBWhiteList){
				db.execute("insert into "+getPrefix()+"DBUser(name,password) values('"+name+"','098f6bcd4621d373cade4e832627b4f6')");
			}
		//}
	}

	private static void createUserGroup(RDBMS db, String tableName) {
		String table = "CREATE TABLE %1$s (\n"
				+ "id BIGINT  NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
				+ "groupName VARCHAR(100) NOT NULL,\n"
				+ "userName VARCHAR(64) NOT NULL,\n"
				+ "createTime TIMESTAMP,\n"
				+ "lastupdatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
//				+ "PRIMARY KEY (id),\n"
//				+ "UNIQUE INDEX groupName_userName (groupName, userName),\n"
//				+ "INDEX userName (userName),\n"
//				+ "INDEX groupName (groupName),\n"
//				+ "CONSTRAINT FK_"+getPrefix()+"DBUserGroup_DBGroup FOREIGN KEY (groupName) REFERENCES "+getPrefix()+"DBGroup (name)\n"
			+ ")\n";

		//if (!db.tableExists(tableName)) {
			db.createTable(tableName, String.format(table, tableName));
			for(String name : DBWhiteList){
				db.execute("insert into "+getPrefix()+"DBUserGroup(groupName,userName) values('admin','"+name+"')");
			}
		
		//}
	}
}
