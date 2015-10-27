/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.resources;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Providers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.www.BasicAuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import com.ebay.pulsar.analytics.auth.exceptions.InvalidSessionException;
import com.ebay.pulsar.analytics.constants.Constants.RequestNameSpace;
import com.ebay.pulsar.analytics.datasource.DataSourceConfiguration;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.DataSourceProvider;
import com.ebay.pulsar.analytics.datasource.DataSourceProviderFactory;
import com.ebay.pulsar.analytics.datasource.DataSourceTypeEnum;
import com.ebay.pulsar.analytics.datasource.DataSourceTypeRegistry;
import com.ebay.pulsar.analytics.datasource.PulsarDataSourceProviderFactory;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.TableDimension;
import com.ebay.pulsar.analytics.datasource.loader.DataSourceConfigurationLoader;
import com.ebay.pulsar.analytics.exception.DataSourceConfigurationException;
import com.ebay.pulsar.analytics.exception.DataSourceException;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.query.SQLQueryProcessor;
import com.ebay.pulsar.analytics.query.request.BaseRequest;
import com.ebay.pulsar.analytics.query.request.BaseSQLRequest;
import com.ebay.pulsar.analytics.query.request.CoreRequest;
import com.ebay.pulsar.analytics.query.request.RealtimeRequest;
import com.ebay.pulsar.analytics.query.request.SQLRequest;
import com.ebay.pulsar.analytics.query.response.TraceAbleResponse;
import com.ebay.pulsar.analytics.query.sql.SimpleTableNameParser;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

@Component
@Scope("request")
@Path("/")
public class PulsarQueryResource {
	private static final Logger logger = LoggerFactory
			.getLogger(PulsarQueryResource.class);
	private static final String SHOWDATASOURCETYPE_TEMPLATE = "show datasourcetypes";
	private static final String SHOWDATASOURCES_TEMPLATE = "show datasources";
	private static final String SHOWDATASOURCE_PREFIX = "show datasources from ";
	private static final String SHOWTABLE_PREFIX = "show tables from ";
	private static final String DESC_TABLE_PREFIX = "desc ";
	
	// JAX-RS context injection
	@Context
	UriInfo info;
	@Context
	HttpHeaders headers;
	@Context
	Request request;
	@Context
	Providers providers;

	// Servlet resource injection as defined by JAX-RS
	@Context
	ServletConfig servletConfig;
	@Context
	ServletContext servletContext;
	@Context
	HttpServletRequest servletRequest;
	@Context
	HttpServletResponse servletResponse;
	@Autowired
	private BasicAuthenticationEntryPoint basicAuthEntryPoint;

	public PulsarQueryResource() {
	}
	
	public boolean isAnonymous() throws BadCredentialsException {
		String userName = getUserName();
		if (Strings.isNullOrEmpty(userName))
			return true;
		return false;
	}
	
	public String getUserName() {
		try {
			Authentication auth = SecurityContextHolder.getContext()
					.getAuthentication();
			if (auth instanceof UsernamePasswordAuthenticationToken) {
				return ((UserDetails) auth.getPrincipal()).getUsername();
			} else if (auth instanceof AnonymousAuthenticationToken) {
				throw new BadCredentialsException("Bad credentials");
			} else {
				throw new BadCredentialsException("Bad credentials");
			}
		} catch (Exception e) {
			throw new BadCredentialsException("Bad credentials");
		}
	}

	public Response handleException(Throwable ex) {
		Status status = Status.BAD_REQUEST;
		if (ex instanceof BadCredentialsException
				|| ex instanceof InvalidSessionException) {
			status = Status.UNAUTHORIZED;
		} else if (ex instanceof AccessDeniedException) {
			status = Status.FORBIDDEN;
		}  else if (ex instanceof DataSourceException
				||ex instanceof DataSourceConfigurationException) {
			status = Status.SERVICE_UNAVAILABLE;
		}  else if (ex instanceof IllegalArgumentException
				|| ex instanceof SqlTranslationException
				|| ex instanceof InvalidQueryParameterException
				|| ex instanceof UnsupportedOperationException) {
			status = Status.BAD_REQUEST;
		}
		Map<String, String> errorMap = new HashMap<String, String>();
		errorMap.put("error", ex.getMessage());

		if (status.equals(Status.UNAUTHORIZED)) {
			return Response
					.status(status)
					.header("WWW-Authenticate",
							"Basic realm=\""
									+ basicAuthEntryPoint.getRealmName() + "\"")
					.entity(errorMap).build();
		}
		return Response.status(status).entity(errorMap).build();

	}

	@POST
	@Path("realtime")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response realtime(@Context HttpServletRequest request,
			RealtimeRequest req) {
		if(logger.isDebugEnabled()){
			logger.debug("Realtime API called from IP: " + request.getRemoteAddr());
		}
		req.setNamespace(RequestNameSpace.realtime);
		boolean trace = request.getParameter("debug") == null ? false : true;
		return processRequest(req, trace);
	}

	@POST
	@Path("core")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response core(@Context HttpServletRequest request, CoreRequest req) {
		if(logger.isDebugEnabled()){
			logger.debug("Core API called from IP: " + request.getRemoteAddr());
		}
		req.setNamespace(RequestNameSpace.core);
		boolean trace = request.getParameter("debug") == null ? false : true;
		return processRequest(req, trace);
	}

	// Wrapper on top of core API for getting yesterday's data
	// "api/yesterday"
	@POST
	@Path("yesterday")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response yesterday(@Context HttpServletRequest request,
			CoreRequest req) {
		if(logger.isDebugEnabled()){
			logger.debug("Yesterday API called from IP: " + request.getRemoteAddr());
		}
		req.setNamespace(RequestNameSpace.yesterday);
		req.setGranularity("fifteen_minute");

		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone("MST"));

		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);

		Date end = new Date(c.getTimeInMillis());

		c.add(Calendar.DATE, -1);
		Date start = new Date(c.getTimeInMillis());

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
				Locale.US);
		dateFormat.setTimeZone(TimeZone.getTimeZone("MST"));

		req.setStartTime(dateFormat.format(start));
		req.setEndTime(dateFormat.format(end));

		boolean trace = request.getParameter("debug") == null ? false : true;
		return processRequest(req, trace);
	}

	private static long MS_15MINS = 900000;

	// Wrapper on top of core API for getting today's data
	@POST
	@Path("today")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response today(@Context HttpServletRequest request, CoreRequest req) {
		if(logger.isDebugEnabled()){
			logger.debug("Today API called from IP: " + request.getRemoteAddr());
		}
		req.setNamespace(RequestNameSpace.today);
		req.setGranularity("fifteen_minute");

		Calendar c = Calendar.getInstance();
		long msEnd = c.getTimeInMillis();
		c.setTimeZone(TimeZone.getTimeZone("MST"));

		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);

		long msStart = c.getTimeInMillis();
		long msDiff = msEnd - msStart;
		
		if(msDiff < 0){
			msStart = msStart- 86400000;
			msDiff = msDiff + 86400000;
		}

		if (msDiff < MS_15MINS) {
			// If the now time is 00:00:00 - 00:14:59 (Round to 0), let's do no
			// rounding.
			if (msDiff < 1000) {
				// If we really have the exact 00:00:00 time, let's just add 1
				// sec for end time.
				msEnd = msStart + 1000;
			} else {
				msEnd = msStart + msDiff;
			}
		} else {
			long msOffset = msDiff / MS_15MINS * MS_15MINS; // normalize to 0,
															// 15, 30, 45min of
															// each hour
			msEnd = msStart + msOffset;
		}
		Date end = new Date(msEnd);

		Date start = new Date(msStart);

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
				Locale.US);
		dateFormat.setTimeZone(TimeZone.getTimeZone("MST"));

		req.setStartTime(dateFormat.format(start));
		req.setEndTime(dateFormat.format(end));

		boolean trace = request.getParameter("debug") == null ? false : true;
		return processRequest(req, trace);
	}

	@POST
	@Path("sql")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response sql(@Context HttpServletRequest request, SQLRequest req) {
		return sql(request, "", req);
	}

	@POST
	@Path("sql/{dataSourceName}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response sql(@Context HttpServletRequest request,
			@PathParam("dataSourceName") String dataSourceName, SQLRequest req) {
		if(logger.isDebugEnabled()){
			logger.debug("SQL API called from IP: " + request.getRemoteAddr());
		}
		req.setNamespace(RequestNameSpace.sql);
		boolean trace = request.getParameter("debug") == null ? false : true;
		return processSqlRequest(req, dataSourceName, trace);
	}

	private Response processRequest(BaseRequest req, boolean trace) {
		Response response = null;
		try {
			long start = System.nanoTime();
			DataSourceProviderFactory factory = DataSourceTypeRegistry.getDataSourceFactory(DataSourceTypeEnum.PULSAR);
			TraceAbleResponse resp = ((PulsarDataSourceProviderFactory)factory).getRestProcessor().executeRestQuery(req);
			if (trace) {
				resp.setRequestProcessTime(System.nanoTime() - start);
				response = Response.ok(resp).build();
			} else {
				response = Response.ok(resp.getQueryResult()).build();
			}
		} catch (Exception ex) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				if (req != null && mapper != null) {
					String str = mapper.writeValueAsString(req);
					if (str != null) {
						logger.debug(str);
					}
				}
			} catch (JsonGenerationException e) {
				logger.warn("JsonGenerationException: " + e);
			} catch (JsonMappingException e) {
				logger.warn("JsonMappingException: " + e);
			} catch (IOException e) {
				logger.warn("IOException: " + e);
			}
			logger.warn("Rest Query Error: " + ex.getMessage());
			return handleException(ex);
		}
		return response;
	}

	private Response processSqlRequest(BaseSQLRequest req, String dataSourceName, boolean trace) {
		Response response = null;
		try {
			if (req.getSql()!= null && !req.getSql().trim().toLowerCase().startsWith("select")) {
				Set<String> dataSourceList = getSourceInfo(req.getSql().trim().toLowerCase());
				GenericEntity<Set<String>> entity = new GenericEntity<Set<String>>(
						dataSourceList) {
				};
				return Response.ok(entity).build();
			}
			long start = System.nanoTime();
			if(Strings.isNullOrEmpty(dataSourceName)){
				String tableName = SimpleTableNameParser.getTableName(req.getSql());
				if(tableName != null){
				int idx = tableName.indexOf('.');
					if(idx > 0){
						dataSourceName = tableName.substring(0, idx);
					}
				}
			}
			if(Strings.isNullOrEmpty(dataSourceName)){
				dataSourceName = DataSourceConfigurationLoader.PULSAR_DATASOURCE;
			}else{
				dataSourceName = dataSourceName.toLowerCase();
			}
			
			if (!dataSourceName.equals(DataSourceConfigurationLoader.PULSAR_DATASOURCE) && isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			
			DataSourceConfiguration configuration = DataSourceMetaRepo.getInstance().getActiveDbConfMap().get(dataSourceName);
			DataSourceTypeEnum dataSourceType = null;
			if(configuration != null){
				dataSourceType = configuration.getDataSourceType();
			}else{
				throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_DATASOURCE.getErrorMessage() + dataSourceName);
			}

			SQLQueryProcessor sqlRequestProcessor = DataSourceTypeRegistry.getDataSourceFactory(dataSourceType).queryProcessor();
			TraceAbleResponse resp = sqlRequestProcessor.executeQuery(req, dataSourceName);
			if (trace) {
				resp.setRequestProcessTime(System.nanoTime() - start);
				response = Response.ok(resp).build();
			} else {
				response = Response.ok(resp.getQueryResult()).build();
			}
		} catch (Exception ex) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				if (req != null && mapper != null) {
					String str = mapper.writeValueAsString(req);
					if (str != null) {
						logger.debug(str);
					}
				}
			} catch (JsonGenerationException e) {
				logger.warn("JsonGenerationException: " + e);
			} catch (JsonMappingException e) {
				logger.warn("JsonMappingException: " + e);
			} catch (IOException e) {
				logger.warn("IOException: " + e);
			}
			logger.warn("SQL Query Error: " + ex.getMessage());
			return handleException(ex);
		}

		return response;
	}

	private Set<String> getSourceInfo(String sql) {
		int length = sql.length();
		if (SHOWDATASOURCETYPE_TEMPLATE.equals(sql)) {
			return getAllDataSourceTypes();
		}else if(SHOWDATASOURCES_TEMPLATE.equals(sql)){
			return getAllActiveDataSources();
		}else if(sql.startsWith(SHOWDATASOURCE_PREFIX)) {
			String datasourceType = sql.substring(SHOWDATASOURCE_PREFIX.length(),
					length);
			if (!Strings.isNullOrEmpty(datasourceType))
				return getActiveDataSourcesFrom(datasourceType.trim());
		}else if (sql.startsWith(SHOWTABLE_PREFIX)) {
			String datasourceName = sql.substring(SHOWTABLE_PREFIX.length(), length);
				return getTablesFromDataSource(datasourceName.trim());
		}else if (sql.startsWith(DESC_TABLE_PREFIX)) {
			String datasourceInfo[] = sql.substring(DESC_TABLE_PREFIX.length(), length).trim().split("\\.");
			Boolean metrics = null;			
			if (datasourceInfo.length >= 2 && datasourceInfo.length <= 3) {
				if (datasourceInfo.length == 3) {
					if ("metrics".equalsIgnoreCase(datasourceInfo[2])) {
						metrics = true;
					} else if ("dimensions".equalsIgnoreCase(datasourceInfo[2])){
						metrics = false;
					} else {
						throw new SqlTranslationException(
								ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage() + "sql text:" + sql);
					}
				}
				return getTableMeta(datasourceInfo[0], datasourceInfo[1], metrics);
			}else 
				throw new SqlTranslationException(ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage() + "sql text:" + sql);
		}
		throw new SqlTranslationException(ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage() + "sql text:" + sql);
	}
	
	@GET
	@Path("listdatasources")
	@Produces(MediaType.APPLICATION_JSON)
	public Response listDatasources(@Context HttpServletRequest request) {
		try {
			Set<String> dataSourceList = getAllActiveDataSources();
			GenericEntity<Set<String>> entity = new GenericEntity<Set<String>>(
					dataSourceList) {
			};
			Status status = Status.OK;

			Response response = Response.status(status).entity(entity).build();
			return response;

		} catch (Exception ex) {
			logger.warn("List Datasources Error: " + ex.getMessage());
			return handleException(ex);
		}
	}

	@GET
	@Path("datasource")
	@Produces(MediaType.APPLICATION_JSON)
	public Response datasourceTable(@Context HttpServletRequest request) {
		return datasourceTable(request, "", "", "");
	}

	@GET
	@Path("datasource/{dataSourceType}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response datasourceTable(@Context HttpServletRequest request,
			@PathParam("dataSourceType") String dataSourceType) {
		return datasourceTable(request, dataSourceType, "", "");
	}

	@GET
	@Path("datasource/{dataSourceType}/{dataSourceName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response datasourceTable(@Context HttpServletRequest request,
			@PathParam("dataSourceType") String dataSourceType,
			@PathParam("dataSourceName") String dataSourceName) {
		return datasourceTable(request, dataSourceType, dataSourceName, "");
	}

	@GET
	@Path("datasource/{dataSourceType}/{dataSourceName}/{table}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response datasourceTable(@Context HttpServletRequest request,
			@PathParam("dataSourceType") String dataSourceType,
			@PathParam("dataSourceName") String dataSourceName,
			@PathParam("table") String table) {
		Response response = null;
		try {
			Set<String> dataSourceList = getDataSourceInfo(request,
					dataSourceType, dataSourceName, table);
			GenericEntity<Set<String>> entity = new GenericEntity<Set<String>>(
					dataSourceList) {
			};
			Status status = Status.OK;
			response = Response.status(status).entity(entity).build();
			return response;

		} catch (Exception ex) {
			logger.warn("Get DataSource Meta Error: " + ex.getMessage());
			return handleException(ex);
		}
	}

	private Set<String> getDataSourceInfo(HttpServletRequest request,
			String dataSourceType, String dataSourceName, String table) {
		if (Strings.isNullOrEmpty(dataSourceType)) {
			return getAllDataSourceTypes();
		} else if (Strings.isNullOrEmpty(dataSourceName)) {
			return getActiveDataSourcesFrom(dataSourceType);
		} else if (Strings.isNullOrEmpty(table)) {
			return getTablesFromDataSource(dataSourceName);
		} else {
			String metricsParamIn = request.getParameter("metrics");
			Boolean metrics = null;
			if (metricsParamIn != null) {
				if (metricsParamIn.equalsIgnoreCase("true")) {
					metrics = true;
				} else if (metricsParamIn.equalsIgnoreCase("false")) {
					metrics = false;
				} else {
					throw new IllegalArgumentException(
							"Invalid metrics value (true|false): "
									+ metricsParamIn);
				}
			}
			return getTableMeta(dataSourceName, table, metrics);
		}
	}

	private Set<String> getTableMeta(String dataSourceName,
			String tableName, Boolean metric) {
		DataSourceProvider DbInstance = DataSourceMetaRepo.getInstance().getDBMetaFromCache(dataSourceName);
		if (DbInstance == null) {
			return ImmutableSet.of();
		}
		Table tableMeta = DbInstance.getTableByName(tableName);
		if (tableMeta == null) {
			return ImmutableSet.of();
		}

		Set<String> result = Sets.newLinkedHashSet();
		if (metric == null || !metric) {
			Set<String> dims = FluentIterable.from(tableMeta.getDimensions())
					.transform(new Function<TableDimension, String>() {
						@Override
						public String apply(TableDimension input) {
							return input.getName();
						}
					}).toSet();
			result.addAll(dims);
		}
		if (metric == null || metric) {
			Set<String> metrics = FluentIterable.from(tableMeta.getMetrics())
					.transform(new Function<TableDimension, String>() {
						@Override
						public String apply(TableDimension input) {
							return input.getName();
						}
					}).toSet();
			result.addAll(metrics);
		}
		return result;
	}

	@PreAuthorize("(#dataSourceName.equals('pulsarholap')) or hasAuthority(#dataSourceName+'_MANAGE') or hasAuthority(#dataSourceName+'_VIEW') or hasAuthority('SYS_MANAGE_DATASOURCE') or hasAuthority('SYS_VIEW_DATASOURCE')")
	private Set<String> getTablesFromDataSource(String dataSource) {
		DataSourceProvider DbInstance = DataSourceMetaRepo.getInstance().getDBMetaFromCache(dataSource);
		if (DbInstance == null) {
			return ImmutableSet.of();
		}
		Collection<Table> tables = DbInstance.getTables();
		if (tables == null) {
			return ImmutableSet.of();
		}
		Set<String> tableNames = FluentIterable.from(tables)
				.transform(new Function<Table, String>() {
					@Override
					public String apply(Table input) {
						return input.getTableName();
					}
				}).toSet();

		return tableNames;
	}

	private Set<String> getAllDataSourceTypes() {
		return DataSourceTypeRegistry.getAllSupportedDataSourceTypes();
	}

	private Set<String> getActiveDataSourcesFrom(String dataSourceType) {
		Set<String> dataSources = Sets.newHashSet();
		for (Map.Entry<String, DataSourceConfiguration> entry : DataSourceMetaRepo.getInstance().getActiveDbConfMap().entrySet()) {
			if(dataSourceType.equalsIgnoreCase(entry.getValue().getDataSourceType().getType()))
			dataSources.add(entry.getKey());
		}
		return dataSources;
	}
	
	private Set<String> getAllActiveDataSources() {
		return DataSourceMetaRepo.getInstance().getActiveDbConfMap().keySet();
	}
}
