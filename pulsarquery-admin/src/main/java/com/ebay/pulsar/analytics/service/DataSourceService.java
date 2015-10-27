/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.access.prepost.PreFilter;
import org.springframework.stereotype.Service;

import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.service.DBDataSourceService;
import com.ebay.pulsar.analytics.dao.service.DBRightGroupService;
import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.DataSourceProvider;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.loader.DynamicDataSourceConfigurationManager;
import com.ebay.pulsar.analytics.security.spring.PermissionControlCache;
import com.ebay.pulsar.analytics.util.JsonUtil;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

/**
 * Service used to manage data sources
 * 
 * @author xinxu1
 * 
 **/
@Service
public class DataSourceService {
	private DBDataSourceService datasourceService;
	private DBRightGroupService rightGroupService;

	public DataSourceService() {
		this.datasourceService = new DBDataSourceService();
		this.rightGroupService = new DBRightGroupService();
	}

//	public String getManagePermission(String datasourceName) {
//		return String.format(PermissionConst.MANAGE_RIGHT_TEMPLATE,
//				datasourceName);
//	}
//
//	public String getTabelPermission(String datasourceName, String tableName) {
//		return String.format(PermissionConst.DATA_TABLE_RIGHT_TEMPLATE,
//				datasourceName, tableName);
//	}

	@PreAuthorize("hasAuthority('ADD_DATASOURCE')")
	public long addDataSource(DBDataSource datasource) {
		checkNotNull(datasource);
		
		checkArgument(
				datasource.getName() != null
						&& !"".equals(datasource.getName()),
				"datasource name could not be empty.");
		checkArgument(
				!isStaticDataSource(datasource.getName()),
				"datasource name["+datasource.getName()+"] is reserved.");
		checkArgument(
				datasource.getType() != null
						&& !"".equals(datasource.getType()),
				"datasource type could not be empty.");
		checkArgument(
				datasource.getEndpoint() != null
						&& !"".equals(datasource.getEndpoint()),
				"datasource endpoint could not be empty.");
		checkArgument(!StringUtils.isEmpty(datasource.getOwner()),
				"datasource owner must be specified.");
		if (datasource.getDisplayName() == null)
			datasource.setDisplayName(datasource.getName());
		if (datasource.getCreateTime() == null)
			datasource.setCreateTime(new Date());
		datasource.setName(datasource.getName());
		DBDataSource dataSourceCondition = new DBDataSource();
		dataSourceCondition.setName(datasource.getName());
		List<DBDataSource> list = datasourceService.get(dataSourceCondition);
		
		checkState(list == null || list.size() == 0,
				"[%s] datasource name already exists.", datasource.getName());
		long id = datasourceService.inser(datasource);
		checkState(id > 0,
				"insert datasource to db failed.datasourceName=%s,owner=",
				datasource.getName(), datasource.getOwner());
		datasource.setId(id);
		DynamicDataSourceConfigurationManager.activateDataSource(datasource);
		PermissionControlCache.getInstance().expireSessions(datasource.getOwner());
		return id;
	}

	@PreAuthorize("hasAuthority(#datasourceName+'_MANAGE') or hasAuthority('SYS_MANAGE_DATASOURCE')")
	public int deleteDataSource(String datasourceName) {

		checkNotNull(datasourceName);
		if(isStaticDataSource(datasourceName)) return 0;
		DBDataSource datasourceCondition = new DBDataSource();
		datasourceCondition.setName(datasourceName);
		List<DBDataSource> list=datasourceService.get(datasourceCondition);
		if(list.size()>0){
			rightGroupService.deleteRightsFromGroupByPrefix(datasourceName + "_");
			int result = datasourceService.deleteBatch(datasourceCondition);
			if (result >= 0) {
				DynamicDataSourceConfigurationManager.disableDataSource(list.get(0));
				PermissionControlCache.getInstance().expireSessionsAll();
			}
			return result;
		}
		return 0;

	}

	@PreFilter("hasAuthority(filterObject+'_MANAGE') or hasAuthority('SYS_MANAGE_DATASOURCE')")
	public int deleteDataSources(List<String> datasourceNames) {
		int rows = 0;
		for (String datasourceName : datasourceNames) {
			try{
			rows += deleteDataSource(datasourceName);
			}catch(Exception e){
			}
		}
		PermissionControlCache.getInstance().expireSessionsAll();
		return rows;
	}

	@PreAuthorize("hasAuthority(#datasource.name+'_MANAGE') or hasAuthority('SYS_MANAGE_DATASOURCE')")
	public int updateDataSource(DBDataSource datasource) {

		checkNotNull(datasource.getName());
		checkArgument(
				!isStaticDataSource(datasource.getName()),
				"datasource ["+datasource.getName()+"] couldn't be updated.");
		DBDataSource datasourceCondition = new DBDataSource();
		datasourceCondition.setName(datasource.getName());
		List<DBDataSource> list = datasourceService.get(datasourceCondition);
		checkState(list.size() == 1, "[%s] datasource is not exists.",
				datasource.getName());
		datasourceCondition.setId(list.get(0).getId());
		if (datasource.getDisplayName() != null) {
			datasourceCondition.setDisplayName(datasource.getDisplayName());
		}
		if (datasource.getEndpoint() != null) {
			datasourceCondition.setEndpoint(datasource.getEndpoint());
		}
		if (datasource.getProperties() != null) {
			datasourceCondition.setProperties(datasource.getProperties());
		}
		DynamicDataSourceConfigurationManager.disableDataSource(list.get(0));
		int response = datasourceService.updateById(datasourceCondition);

		DBDataSource fromDB = getDataSourceByName(datasource.getName());
		DynamicDataSourceConfigurationManager.activateDataSource(fromDB);
		return response;
	}

	@PreAuthorize("hasAuthority(#datasourceName+'_MANAGE') or hasAuthority(#datasourceName+'_VIEW') or hasAuthority('SYS_MANAGE_DATASOURCE') or hasAuthority('SYS_VIEW_DATASOURCE')")
	public DBDataSource getDataSourceByName(String datasourceName) {

		checkNotNull(datasourceName);
		DBDataSource datasourceCondition = new DBDataSource();
		datasourceCondition.setName(datasourceName);
		List<DBDataSource> list = datasourceService.get(datasourceCondition);
		checkState(list.size() == 1, "[%s] datasource is not exists.",
				datasourceName);
		return list.get(0);
	}

	@PreAuthorize("hasAuthority(#datasourceName+'_MANAGE') or hasAuthority(#datasourceName+'_VIEW')")
	public List<String> getDataSourceTables(DBDataSource datasource) {

		StringBuilder keyBuilder = new StringBuilder();
		Joiner.on('.').appendTo(keyBuilder, datasource.getType(),
				datasource.getName());
		String dbNameSpace = keyBuilder.toString();
		DataSourceProvider dataSourceProvider = DataSourceMetaRepo
				.getInstance().getDBMetaFromCache(dbNameSpace);
		if (dataSourceProvider != null) {
			return FluentIterable.from(dataSourceProvider.getTables())
					.transform(new Function<Table, String>() {
						public String apply(Table input) {
							return input.getTableName();
						}
					}).toList();
		}
		return null;

	}

	public List<String> getAllDataSourcesForOwner(String userName) {

		checkNotNull(userName);
		DBDataSource datasourceCondition = new DBDataSource();
		datasourceCondition.setOwner(userName);
		return Lists.transform(datasourceService.get(datasourceCondition),
				new Function<DBDataSource, String>() {
					@Override
					public String apply(DBDataSource input) {
						return input.getName();
					}
				});
	}

	@PreFilter("hasAuthority(filterObject+'_MANAGE') or hasAuthority('SYS_MANAGE_DATASOURCE') or hasAuthority('SYS_VIEW_DATASOURCE')")
	public List<DBDataSource> getDataSourceByNames(List<String> names) {
		return datasourceService.getAllByColumnIn("name", names, -1);
	}

	@PostFilter("hasAuthority(filterObject.name+'_MANAGE') or hasAuthority(filterObject.name+'_VIEW') or hasAuthority('SYS_MANAGE_DATASOURCE') or hasAuthority('SYS_VIEW_DATASOURCE')")
	public Set<DBDataSource> getAllUserViewedDatasource() {

		Set<DBDataSource> datasources = new HashSet<DBDataSource>();
		for (Entry<String, DataSourceConfiguration> entry : DataSourceMetaRepo
				.getInstance().getActiveDbConfMap().entrySet()) {
			if (entry.getValue().isRealOnly() == true) {
				DBDataSource datasource = new DBDataSource();
				datasource.setName(entry.getKey());
				datasource.setType(entry.getValue().getDataSourceType()
						.getType());
				datasource.setDisplayName(entry.getValue().getDataSourceName());
				datasource.setEndpoint((StringUtils.join(entry.getValue()
						.getEndPoint(), ",")));
				try{
					datasource.setProperties(JsonUtil.writeValueAsIndentString(entry.getValue().getProperties()));
				}catch(Exception e){
					datasource.setProperties(entry.getValue().getProperties().toString());
				}
				datasource.setReadonly(entry.getValue().isRealOnly());
				datasources.add(datasource);
			}
		}
		datasources.addAll(datasourceService.getAll());
		return datasources;
	}

	@PostFilter("hasAuthority(filterObject.name+'_MANAGE') or hasAuthority('SYS_MANAGE_DATASOURCE')")
	public Set<DBDataSource> getAllUserManagedDatasource() {
		Set<DBDataSource> datasources = new HashSet<DBDataSource>();
		for (Entry<String, DataSourceConfiguration> entry : DataSourceMetaRepo
				.getInstance().getDbConfMap().entrySet()) {
			DataSourceConfiguration conf=entry.getValue();
			if(conf.isRealOnly()){
				DBDataSource datasource = new DBDataSource();
				datasource.setName(entry.getKey());
				datasource.setType(entry.getValue().getDataSourceType().getType());
				datasource.setDisplayName(entry.getValue().getDataSourceName());
				datasource.setEndpoint((StringUtils.join(entry.getValue()
						.getEndPoint(), ",")));
				try{
					datasource.setProperties(JsonUtil.writeValueAsIndentString(entry.getValue().getProperties()));
				}catch(Exception e){
					datasource.setProperties(entry.getValue().getProperties().toString());
				}
				datasource.setReadonly(entry.getValue().isRealOnly());
				datasources.add(datasource);
			}
		}
		datasources.addAll(datasourceService.getAll());
		return datasources;
	}

	private boolean isStaticDataSource(String name){
		for (Entry<String, DataSourceConfiguration> entry : DataSourceMetaRepo
				.getInstance().getDbConfMap().entrySet()) {
			DataSourceConfiguration conf=entry.getValue();
			if(conf.isRealOnly()){
				if(entry.getKey().equals(name)) return true;
			}
		}
		return false;
	}
	
}
