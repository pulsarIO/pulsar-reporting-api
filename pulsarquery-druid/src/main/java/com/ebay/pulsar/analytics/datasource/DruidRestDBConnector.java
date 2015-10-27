/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;


import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.analytics.exception.DataSourceException;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.BaseQuery;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.ebay.pulsar.analytics.query.request.PulsarDateTimeFormatter;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidRestDBConnector implements DBConnector {
	private static final int ONE_HOUR = 3600000;
	private static final int THREE_HOUR = ONE_HOUR * 3;
	private static final int FOUR_HOUR = ONE_HOUR * 4;
	private static final int ONE_DAY = ONE_HOUR * 24;
	
	private static final Logger logger = LoggerFactory.getLogger(DruidRestDBConnector.class);
	private static String DATASOURCE = "datasources";
	private static String DRUIDVERSION = "0.7.1.1";
	
	private DataSourceConfiguration configuration;
	private Client druidRestClient;
	private List<WebTarget> targetList;
	private int targetSize;
	private AtomicLong callCounter = new AtomicLong();
	private AtomicBoolean closed = new AtomicBoolean(false);
	
	public DruidRestDBConnector(DataSourceConfiguration configuration){
		this.configuration = configuration;
		start();
	}
	
	@Override
	public void start() {
		String endPoint = configuration.getEndPoint().get(0);
		if (endPoint.startsWith("https")) {
			try {
				druidRestClient = ClientHelper.initSSLClient();
			} catch (KeyManagementException e) {
				throw new DataSourceException("Druid invalid endpoint.");
			} catch (NoSuchAlgorithmException e) {
				throw new DataSourceException("Druid invalid endpoint.");
			}
		} else {
			druidRestClient = ClientHelper.initClient();
		}
		targetList =
				FluentIterable.from(configuration.getEndPoint()).transform(new Function<String, WebTarget>() {
					@Override
					public WebTarget apply(String input) {
						return druidRestClient.target(input);
				}
				}).toList();
		targetSize = targetList.size();
	}
	
	public String sendQuery(BaseQuery query) {
		checkState();
		if(targetSize == 0)
			throw new DataSourceException("No available Druid Broker Servers.");
		
		// Move to the next node first for dynamic load balance purpose
		WebTarget webTarget = targetList.get((int)(callCounter.incrementAndGet() % targetSize));
		try {
			return sendQueryToEndpoint(query, webTarget);
		} catch (Exception e) {
			logger.warn("Query Druid error:" + e);
		}
		throw new DataSourceException("Druid server error: Query Error!");
	}
	
	public String sendQuery(BaseQuery query, String brokerEndPoint) {
		checkState();
		WebTarget webTarget = druidRestClient.target(brokerEndPoint);
		try {
			return sendQueryToEndpoint(query, webTarget);
		} catch (Exception e) {
			logger.warn("Query Druid error:" + e);
		}
		throw new DataSourceException("Druid server error: Query Error!");
	}
	
	@Override
	public Object query(Object query){
		return sendQuery((BaseQuery)query);
	}

	@Override
	public Set<String> getAllTables() {
		checkState();
		if(targetSize == 0)
			throw new DataSourceException("No available Druid Broker Servers.");
		WebTarget webTarget = targetList.get(0);
		return getDruidTables(webTarget);
	}
	
	public Set<String> getAllTables(String brokerEndPoint){
		checkState();
		WebTarget webTarget = druidRestClient.target(brokerEndPoint);
		return getDruidTables(webTarget);
	}
	
	@Override
	public Table getTableMeta(String tableName){
		checkState();
		if (tableName == null || tableName.isEmpty()) {
			return null;
		}
		if(targetSize == 0)
			throw new DataSourceException("No available Druid Broker Servers.");
		WebTarget webTarget = targetList.get(0);
		DruidRestTableMeta columnsMeta = getDruidTableMeta(tableName, webTarget);
		return populateTableMeta(tableName, columnsMeta);
	}

	public Table getTableMeta(String tableName, String brokerEndPoint) {
		checkState();
		if (tableName == null || tableName.isEmpty()) {
			return null;
		}
		WebTarget webTarget = druidRestClient.target(brokerEndPoint);
		DruidRestTableMeta columnsMeta = getDruidTableMeta(tableName, webTarget);
		return populateTableMeta(tableName, columnsMeta);
	}
	
	private String sendQueryToEndpoint(BaseQuery query, WebTarget webTarget) {
		WebTarget webResource = webTarget.path("/").queryParam("pretty");
		Builder builder = webResource.request(MediaType.APPLICATION_JSON_TYPE);

		builder.accept(MediaType.APPLICATION_JSON);
		Response response = builder.post(Entity.entity(query.toString(), MediaType.APPLICATION_JSON));

		int statusCode = response.getStatus();
		String responseBody = "";
		if (statusCode == Status.OK.getStatusCode()) {
			responseBody = response.readEntity(String.class);
		} else {
			String errorMsg = "Druid HTTP Status Code - " + statusCode + "; Response - " + response.readEntity(String.class) + "; Query - " + query.toString();
			logger.warn (errorMsg);
			throw new DataSourceException(errorMsg);
		}
		return responseBody;
	}
	
	@SuppressWarnings("unchecked")
	private Set<String> getDruidTables(WebTarget webTarget) {
		WebTarget dataSourceRs = webTarget.path(DATASOURCE);
		Builder builder = dataSourceRs.request(MediaType.APPLICATION_JSON_TYPE);
		builder.accept(MediaType.APPLICATION_JSON);
		Response response = builder.get();

		int statusCode = response.getStatus();
		Set<String> result = null;
		if (statusCode == Status.OK.getStatusCode()) {
			result = response.readEntity(Set.class);
		} else {
			String errorMsg = "Druid HTTP Status Code - " + statusCode + "; Response - " + response.readEntity(String.class) + "; GET - " + webTarget.getUri();
			logger.warn (errorMsg);
			throw new DataSourceException(errorMsg);
		}
		return result;
	}

	private DruidRestTableMeta getDruidTableMeta(String tableName, WebTarget webTarget) {
		WebTarget tableResource = webTarget.path(DATASOURCE).path(tableName);
		Builder builder = tableResource.request(MediaType.APPLICATION_JSON_TYPE);
		builder.accept(MediaType.APPLICATION_JSON);
		Response response = builder.get();
		
		int statusCode = response.getStatus();
		DruidRestTableMeta result = null;
		if (statusCode == Status.OK.getStatusCode()) {
			result = response.readEntity(DruidRestTableMeta.class);
		} else {
			String errorMsg = "Druid HTTP Status Code - " + statusCode + "; Response - " + response.readEntity(String.class) + "; GET - " + webTarget.getUri();
			logger.warn (errorMsg);
			throw new DataSourceException(errorMsg);
		}
		return result;
	}
	
	@SuppressWarnings("rawtypes")
	private String getDruidVersion() {
		checkState();
		if(targetSize == 0)
			throw new DataSourceException("No available Druid Broker Servers.");
		List<WebTarget> druidHostList =
				FluentIterable.from(configuration.getEndPoint()).transform(new Function<String, WebTarget>() {
					@Override
					public WebTarget apply(String input) {
						return druidRestClient.target(input.replace("druid/v2", "status"));
				}
				}).toList();
		WebTarget webTarget = druidHostList.get(0);
		Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
		builder.accept(MediaType.APPLICATION_JSON);
		Response response = builder.get();
		
		int statusCode = response.getStatus();
		Map result = null;
		String version = "";
		if (statusCode == Status.OK.getStatusCode()) {
			result = response.readEntity(Map.class);
			if(result.size() > 0 && result.containsKey("version")){
				version = result.get("version").toString();
			}
		} else {
			String errorMsg = "Druid HTTP Status Code - " + statusCode + "; Response - " + response.readEntity(String.class) + "; GET - " + webTarget.getUri();
			logger.warn (errorMsg);
			throw new DataSourceException(errorMsg);
		}
		return version;
	}
	

	@SuppressWarnings("unchecked")
	private Map<String, Object> getDruidTableMetaType(String tableName) {
		checkState();
		if(targetSize == 0)
			throw new DataSourceException("No available Druid Broker Servers.");
		WebTarget webTarget = targetList.get(0);
		
		
		WebTarget queryTarget=webTarget.path("/").queryParam("pretty");
		Builder builder = queryTarget.request(MediaType.APPLICATION_JSON_TYPE);
		builder.accept(MediaType.APPLICATION_JSON);
		
		long checkPoint  = (System.currentTimeMillis()/ONE_HOUR)*ONE_HOUR;
		Response response = getCheckPointSegmentMeta(tableName, builder, checkPoint);
		int statusCode = response.getStatus();
		if(statusCode != Status.OK.getStatusCode()){
			response = getCheckPointSegmentMeta(tableName, builder, checkPoint - ONE_DAY);
			statusCode = response.getStatus();
		}
		List<Map<String, Object>> result = null;
		Map<String, Object> columnTypes = Maps.newHashMap();
		Map<String, Object> columns = Maps.newHashMap();
		if (statusCode == Status.OK.getStatusCode()) {
			result = response.readEntity(List.class);	
			if (result.size() <= 0 || !result.get(0).containsKey("columns")) {
				response = getCheckPointSegmentMeta(tableName, builder,
						checkPoint - ONE_DAY);
				result = response.readEntity(List.class);
			}
			if (result.size() > 0 && result.get(0).containsKey("columns")) {
				columns = (Map<String, Object>) result.get(0).get("columns");
				for (String key : columns.keySet()) {
					columnTypes.put(key, ((Map<String, Object>)columns.get(key)).get("type"));
				}
			}

		} else {
			String errorMsg = "Druid HTTP Status Code - " + statusCode + "; Response - " + response.readEntity(String.class) + "; GET - " + webTarget.getUri();
			logger.warn (errorMsg);
		}
		return columnTypes;
	}

	private Response getCheckPointSegmentMeta(String tableName, Builder builder,long checkPoint) {
		DateTime startTime = new DateTime(checkPoint - FOUR_HOUR);
		DateTime endTime = new DateTime(checkPoint - THREE_HOUR);
		DateRange dataRange = new DateRange(startTime, endTime);
		List<String> intervals=PulsarDateTimeFormatter.buildStringIntervals(dataRange);
		String query="{\"queryType\":\"segmentMetadata\",\"dataSource\":\"" + tableName +"\",\"intervals\":[\"" + intervals.get(0) +"\"]}";
		Response response = builder.post(Entity.entity(query, MediaType.APPLICATION_JSON));
		return response;
	}
	

	

	private Table populateTableMeta(String tableName, DruidRestTableMeta columnsMeta) {
		if(columnsMeta == null)
			return null;
		
		Set<String> dimensions = columnsMeta.getDimensions();
		Set<String> metrics = columnsMeta.getMetrics();
		if((dimensions == null || dimensions.size() == 0) &&  (metrics == null || metrics.size() == 0 )){
			return null;
		}
		Table tableMeta = new Table();
		tableMeta.setTableName(tableName.toLowerCase());
		final String druidVersion = getDruidVersion();
		final Map<String, Object> tableMetaTypes = getDruidTableMetaType(tableName);
		if (dimensions != null && dimensions.size() > 0) {
			List<TableDimension> allDimensions = FluentIterable.from(dimensions)
					.transform(new Function<String, TableDimension>() {
						@Override
						public TableDimension apply(String input) {
							if(compareTo(druidVersion, DRUIDVERSION) >= 0 && tableMetaTypes.containsKey(input)){
								return new TableDimension(input, getDimDataType(tableMetaTypes.get(input).toString()));
							}
							else {
								return new TableDimension(input, Table.VARCHAR);
							}
						}
					}).toList();
			tableMeta.setDimensions(allDimensions);
		}
		
		if (metrics != null && metrics.size() > 0) {
			List<TableDimension> allMetrics = FluentIterable.from(metrics)
					.transform(new Function<String, TableDimension>() {
						@Override
						public TableDimension apply(String input) {
							if(compareTo(druidVersion, DRUIDVERSION) >= 0 && tableMetaTypes.containsKey(input)){
								return new TableDimension(input, getDimDataType(tableMetaTypes.get(input).toString()));
							}
							else {
								return new TableDimension(input, Table.BIGINT);
							}
						}
					}).toList();
			tableMeta.setMetrics(allMetrics);
		}
		return tableMeta;
	}
	
	public int getDimDataType(String druidDataType) {
		int type;
		switch (druidDataType) {
		case "TINYINT":
			type = Table.BIGINT;
			break;
		case "SMALLINT":
			type = Table.BIGINT;
			break;
		case "INTEGER":
			type = Table.BIGINT;
			break;
		case "BIGINT":
			type = Table.BIGINT;
			break;
		case "FLOAT":
			type = Table.DOUBLE;
			break;
		case "LONG":
			type = Table.DOUBLE;
			break;
		case "DOUBLE":
			type = Table.DOUBLE;
			break;
		case "NUMERIC":
			type = Table.DOUBLE;
			break;
		case "DECIMAL":
			type = Table.DOUBLE;
			break;
		default:
			type = Table.VARCHAR;
		}
		return type;
	}

	public int compareTo(String currentVersion, String defultVersion) {
		if (Strings.isNullOrEmpty(currentVersion) || Strings.isNullOrEmpty(defultVersion)) {
			return -1;
		}
		currentVersion = currentVersion.replaceAll("[^0-9.]+", "");
		defultVersion = defultVersion.replaceAll("[^0-9.]+", "");
		String[] currentParts = currentVersion.split("\\.");
		String[] defultParts = defultVersion.split("\\.");
		int length = Math.max(currentParts.length, defultParts.length);
		for (int i = 0; i < length; i++) {
			int currentPart = i < currentParts.length ? Integer
					.parseInt(currentParts[i]) : 0;
			int defultPart = i < currentParts.length ? Integer
					.parseInt(defultParts[i]) : 0;
			if (currentPart < defultPart)
				return -1;
			if (currentPart > defultPart)
				return 1;
		}
		return 0;
	}

	@Override
	public void close() {
		druidRestClient.close();
		druidRestClient = null;
		closed.set(true);
	}
	
	private void checkState(){
		if(closed.get() == true){
			throw new DataSourceException("DBConnector closed!");
		}
	}

	
}
