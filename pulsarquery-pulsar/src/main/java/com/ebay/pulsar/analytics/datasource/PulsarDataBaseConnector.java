/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/


package com.ebay.pulsar.analytics.datasource;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ebay.pulsar.analytics.exception.DataSourceConfigurationException;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarDataBaseConnector implements DBConnector {
	private PulsarDataSourceConfiguration configuration;
	private DataSourceProvider rtolap;
	private DataSourceProvider molap;
	private DataSourceRoutingStrategy routingStrategy;
	
	private AtomicBoolean closed = new AtomicBoolean(false);
	
	public PulsarDataBaseConnector(PulsarDataSourceConfiguration configuration) {
		this.configuration = configuration;
		start();
	}
	
	@Override
	public void start() {
		if(Strings.isNullOrEmpty(configuration.getRtolapName())){
			throw new DataSourceConfigurationException("Error configuration, rtolap datasource not found!");
		}
		this.rtolap = DataSourceMetaRepo.getInstance().getDBMetaFromCache(configuration.getRtolapName());
		if(this.rtolap == null){
			throw new DataSourceConfigurationException("Error configuration, rtolap datasource not found!");
		}
		if(!Strings.isNullOrEmpty(configuration.getMolapName())){
			this.molap = DataSourceMetaRepo.getInstance().getDBMetaFromCache(configuration.getMolapName());
		}
		PulsarDataSourceRoutingStrategy  strategy = new PulsarDataSourceRoutingStrategy();
		strategy.setConfiguration(configuration.getRoutConfiguration());
		this.routingStrategy = strategy;
	}
	
	@Override
	public Object query(Object query) {
		return sendQuery(query);
	}
	
	public String sendQuery(Object query) {
		checkState();
		DataSourceProvider db = rtolap;
		return (String)db.getConnector().query(query);
	}
	
	public String getOLAPByTableAndIntervals(String tableName, DateRange intervals){
		String dbName = routingStrategy.getDataSourceName(tableName, intervals, configuration.getMolapName(), configuration.getRtolapName());
		return dbName;
	}
	
	public String getrtolapName(){
		return configuration.getRtolapName();
	}
	
	public String getMOLAPName(){
		return configuration.getMolapName();
	}
	
	@Override
	public Set<String> getAllTables() {
		checkState();
		Set<String> pulsarTables = Sets.newHashSet();
		Set<String> selectedRTOLAPTables = Sets.newHashSet();
		for(PulsarTable tableFromConf : configuration.getPulsarTableConfiguration()){
			if(isExecluded(tableFromConf)){
				continue;
			}			
			//Check the table names in RTOLAP and MOLAP, now we only support 1:1 mapping to individual OLAP.
			Table rtolapTable = null;
			Table molapTable = null;
			if(tableFromConf.getRTOLAPTableName() != null){
				rtolapTable = rtolap.getTableByName(tableFromConf.getRTOLAPTableName());
				if(rtolapTable == null){
					throw new DataSourceConfigurationException("Error configuration, rtolap table not found!");
				}
			}
			
			if(tableFromConf.getMOLAPTableName() != null){
				if(molap == null){
					throw new DataSourceConfigurationException("Error configuration, molap table not found!");
				}
				molapTable = molap.getTableByName(tableFromConf.getMOLAPTableName());
				if(molapTable == null){
					throw new DataSourceConfigurationException("Error configuration, molap table not found!");
				}
			}
			
			for (String tableName : tableFromConf.getAllTableNames()) {
				if (rtolapTable == null) {
					rtolapTable = rtolap.getTableByName(tableName);
				}
				if (molap != null && molapTable == null) {
					molapTable = molap.getTableByName(tableName);
				}
			}
			if(rtolapTable == null){
				throw new DataSourceConfigurationException("Error configuration, rtolap table not found!");
			}else{
				selectedRTOLAPTables.add(rtolapTable.getTableName());
			}
	
			pulsarTables.add(tableFromConf.getTableName());
		}
		

		Collection<Table> allRTOLAPTables = rtolap.getTables();
		for(Table rtolapTable : allRTOLAPTables){
			if(!isExecluded(rtolapTable)){
				if(!selectedRTOLAPTables.contains(rtolapTable.getTableName())){
					pulsarTables.add(rtolapTable.getTableName());
				}
			}
		}
		return pulsarTables;
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public PulsarTable getTableMeta(String tableName) {
		checkState();
		if (tableName == null || tableName.isEmpty()) {
			return null;
		}
		PulsarTable selectedTable = null;
		for(PulsarTable tableFromConf : configuration.getPulsarTableConfiguration()){
			if(tableFromConf.getAllTableNames().contains(tableName)){
				selectedTable = tableFromConf;
			}
			if(selectedTable !=null && isExecluded(selectedTable)){
				selectedTable = null;
			}
			if(selectedTable != null){
				break;
			}
		}
		
		if(selectedTable != null){
			PulsarTable pulsarTable = new PulsarTable();
			//Copy
			pulsarTable.setTableName(selectedTable.getTableName());
			if(selectedTable.getTableNameAlias() != null){
				pulsarTable.setTableNameAlias(selectedTable.getTableNameAlias());
			}
			
			List<PulsarTableDimension> pulsarTabDims = Lists.newArrayList();
			if(selectedTable.getDimensions() != null)
				pulsarTabDims.addAll((Collection<PulsarTableDimension>) selectedTable.getDimensions());
			pulsarTable.setDimensions(pulsarTabDims);
			
			List<PulsarTableDimension> pulsarTabMetrics = Lists.newArrayList();
			if(selectedTable.getMetrics() != null)
				pulsarTabMetrics.addAll((Collection<PulsarTableDimension>) selectedTable.getMetrics());
			pulsarTable.setMetrics(pulsarTabMetrics);

			//Check the table names in rtolap and MOLAP, now we only support 1:1 mapping to individual OLAP.
			Table rtolapTable = null;
			Table molapTable = null;
			if(selectedTable.getRTOLAPTableName() != null){
				rtolapTable = rtolap.getTableByName(selectedTable.getRTOLAPTableName());
				if(rtolapTable == null){
					throw new DataSourceConfigurationException("Error configuration, rtolap table not found!");
				}
			}
			
			if(selectedTable.getMOLAPTableName() != null){
				if(molap == null){
					throw new DataSourceConfigurationException("Error configuration, molap table not found!");
				}
				molapTable = molap.getTableByName(selectedTable.getMOLAPTableName());
				if(molapTable == null){
					throw new DataSourceConfigurationException("Error configuration, molapTable table not found!");
				}
			}
			
			for (String table : selectedTable.getAllTableNames()) {
				if (rtolapTable == null) {
					rtolapTable = rtolap.getTableByName(table);
				}
				if (molap != null && molapTable == null) {
					molapTable = molap.getTableByName(table);
				}
			}
			if(rtolapTable == null){
				throw new DataSourceConfigurationException("Error configuration, rtolap table not found!");
			}
			
			pulsarTable.setRTOLAPTableName(rtolapTable.getTableName());
			if(molapTable != null)
				pulsarTable.setMOLAPTableName(molapTable.getTableName());
			
			//Merge the table meta Data
			mergeDimension(pulsarTable, rtolapTable, true);
			mergeMetric(pulsarTable, rtolapTable, true);
			
			if(molapTable != null){
				mergeDimension(pulsarTable, molapTable, false);
				mergeMetric(pulsarTable, molapTable, false);
			}
			return pulsarTable;
		}
		else{
			Table rtolapTable = rtolap.getTableByName(tableName);
			if(!isExecluded(rtolapTable)){
				PulsarTable pulsarTable =  new PulsarTable(rtolapTable);
				pulsarTable.setRTOLAPTableName(rtolapTable.getTableName());
				return pulsarTable;
			}
		}
		return null;
	}
	
	private void mergeDimension(PulsarTable pulsarTable, Table olapTable, boolean rtolap) {
		for(TableDimension dbDim : olapTable.getDimensions()){
			if(pulsarTable.getDimensionByName(dbDim.getName()) == null){
				PulsarTableDimension pulsarDim =  new PulsarTableDimension(dbDim.getName(), dbDim.getType());
				if(rtolap){
					pulsarDim.setRTOLAPColumnName(dbDim.getName());
				}else{
					pulsarDim.setMOLAPColumnName(dbDim.getName());
				}
				@SuppressWarnings("unchecked")
				Collection<PulsarTableDimension> dimensions= (Collection<PulsarTableDimension>)pulsarTable.getDimensions();
				if(pulsarTable.insertDimensionMap(pulsarDim)){
					dimensions.add(pulsarDim);
				}
			}else{
				PulsarTableDimension pulsarDim = (PulsarTableDimension)pulsarTable.getDimensionByName(dbDim.getName());
				if(rtolap){
					pulsarDim.setRTOLAPColumnName(dbDim.getName());
				}else{
					pulsarDim.setMOLAPColumnName(dbDim.getName());
					//Override with MOLAP's datatype
					pulsarDim.setType(dbDim.getType());
				}
			}
		}
	}

	private void mergeMetric(PulsarTable pulsarTable, Table olapTable, boolean rtolap) {
		for(TableDimension dbMetric : olapTable.getMetrics()){
			if(pulsarTable.getMetricByName(dbMetric.getName()) == null){
				PulsarTableDimension pulsarMetric =  new PulsarTableDimension(dbMetric.getName(), dbMetric.getType());
				if(rtolap){
					pulsarMetric.setRTOLAPColumnName(dbMetric.getName());
				}else{
					pulsarMetric.setMOLAPColumnName(dbMetric.getName());
				}
				@SuppressWarnings("unchecked")
				Collection<PulsarTableDimension> metrics= (Collection<PulsarTableDimension>)pulsarTable.getMetrics();
				if(pulsarTable.insertMetricMap(pulsarMetric)){
					metrics.add(pulsarMetric);
				}
			}else{
				PulsarTableDimension pulsarDim = (PulsarTableDimension) pulsarTable.getMetricByName(dbMetric.getName());
				if (rtolap) {
					pulsarDim.setRTOLAPColumnName(dbMetric.getName());
				} else {
					pulsarDim.setMOLAPColumnName(dbMetric.getName());
					//Override with MOLAP's datatype
					pulsarDim.setType(dbMetric.getType());
				}
			}
		}
	}
	
	private boolean isExecluded(Table table){
		boolean excluded = false;
		if (configuration.getExcludeTableSets() != null) {
			if (Sets.intersection(configuration.getExcludeTableSets(), table.getAllTableNames()).size() > 0) {
				excluded = true;
			}
		}
		
		if(table.getDimensions() == null || table.getDimensions().isEmpty()){
			excluded = true;
		}
		return excluded;
	}

	@Override
	public void close() {
		rtolap.getConnector().close();
		if(molap != null){
			molap.getConnector().close();
		}
		closed.set(true);
	}
	
	private void checkState(){
		if(closed.get() == true){
			throw new  IllegalStateException("DBConnector closed!");
		}
	}
}
