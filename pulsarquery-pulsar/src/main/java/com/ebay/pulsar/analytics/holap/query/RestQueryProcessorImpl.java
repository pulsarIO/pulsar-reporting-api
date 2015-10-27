/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.holap.query;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.InitializingBean;

import com.ebay.pulsar.analytics.cache.CacheProvider;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.DataSourceProvider;
import com.ebay.pulsar.analytics.datasource.PulsarRestMetricMeta;
import com.ebay.pulsar.analytics.datasource.PulsarRestMetricRegistry;
import com.ebay.pulsar.analytics.datasource.PulsarTable;
import com.ebay.pulsar.analytics.datasource.PulsarTableDimension;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.loader.DataSourceConfigurationLoader;
import com.ebay.pulsar.analytics.exception.DataSourceConfigurationException;
import com.ebay.pulsar.analytics.exception.DataSourceException;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.holap.query.model.QueryMetricProps;
import com.ebay.pulsar.analytics.holap.query.validator.PulsarRestValidator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.HyperUniqueAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.filter.AndFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.DruidGranularityParser;
import com.ebay.pulsar.analytics.metricstore.druid.having.AndHaving;
import com.ebay.pulsar.analytics.metricstore.druid.having.BaseHaving;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.query.DruidQueryParameter;
import com.ebay.pulsar.analytics.metricstore.druid.query.DruidQueryProcessor;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.DruidSpecs;
import com.ebay.pulsar.analytics.metricstore.druid.query.validator.DruidGranularityValidator;
import com.ebay.pulsar.analytics.metricstore.druid.query.validator.GranularityAndTimeRange;
import com.ebay.pulsar.analytics.query.AbstractSQLQueryProcessor;
import com.ebay.pulsar.analytics.query.RestQueryProcessor;
import com.ebay.pulsar.analytics.query.SQLQueryContext;
import com.ebay.pulsar.analytics.query.request.BaseRequest;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.ebay.pulsar.analytics.query.response.TraceAbleResponse;
import com.ebay.pulsar.analytics.query.sql.SQLTranslator;
import com.ebay.pulsar.analytics.query.validator.QueryValidator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 * @author mingmwang
 *
 */
public class RestQueryProcessorImpl extends AbstractSQLQueryProcessor implements RestQueryProcessor, InitializingBean{
	private QueryValidator<GranularityAndTimeRange> druidValidator = new DruidGranularityValidator();
	private DruidFilterParser druidFilterParser =  new DruidFilterParser();
	private DruidHavingParser druidHavingParser =  new DruidHavingParser();
	
	
	private QueryValidator<BaseRequest> restValidator;
	private CacheProvider cacheProvider;
	private PulsarRestMetricRegistry pulsarRestMetricRegistry;

	private DruidQueryProcessor druidQueryProcessor;

	public CacheProvider getCacheProvider() {
		return cacheProvider;
	}

	public void setCacheProvider(CacheProvider cacheProvider) {
		this.cacheProvider = cacheProvider;
	}

	public PulsarRestMetricRegistry getPulsarRestMetricRegistry() {
		return pulsarRestMetricRegistry;
	}

	public void setPulsarRestMetricRegistry(
			PulsarRestMetricRegistry pulsarRestMetricRegistry) {
		this.pulsarRestMetricRegistry = pulsarRestMetricRegistry;
	}
	
	@Override
	public TraceAbleResponse executeRestQuery(BaseRequest req)
			throws JsonParseException, JsonMappingException, IOException, ParseException, DataSourceConfigurationException,DataSourceException,
			SqlTranslationException,InvalidQueryParameterException {
		restValidator.validate(req);
		
		// Only Dealing with 1 metric now
		String metric = req.getMetrics().get(0).toLowerCase();
		
		DateRange intervals = req.getQueryDateRange();
		PulsarRestMetricMeta metricMeta = pulsarRestMetricRegistry.getMetricsMetaFromName(metric);
		QueryMetricProps metricProps = new QueryMetricProps(metricMeta, intervals);
		
		String tableName = metricMeta.getTableName();
		DataSourceProvider pulsarDB = getPulsarDB();
		PulsarTable tableMeta = (PulsarTable)pulsarDB.getTableByName(tableName);
		BaseGranularity granularity = DruidGranularityParser.parse(req.getGranularity());
		druidValidator.validate(new GranularityAndTimeRange(granularity, req.getQueryDateRange()));
		return queryDruidRest(req, metricProps, granularity, tableMeta);
	}

	private TraceAbleResponse queryDruidRest(BaseRequest req, QueryMetricProps metricProps, BaseGranularity granularity, final PulsarTable tableMeta) throws JsonParseException, JsonMappingException, IOException, ParseException,DataSourceConfigurationException,DataSourceException,
			SqlTranslationException,InvalidQueryParameterException {
		String tableName = tableMeta.getRTOLAPTableName();
		PulsarRestMetricMeta metricMeta = metricProps.getMetricMeta();
		
		BaseFilter userFilter = null;
	    String reqFilter = req.getFilter();
	    if (reqFilter != null && !(reqFilter.trim()).isEmpty()) {
		   userFilter = druidFilterParser.parseWhere(req.getFilter(), tableMeta);
		   if(userFilter == null) {
				throw new SqlTranslationException(ExceptionErrorCode.INVALID_FILTER.getErrorMessage() + req.getFilter());
		   }
	    }
		
		BaseFilter finalFilter = null;
		if(userFilter != null && metricMeta.getDruidFilter() != null) {
			List<BaseFilter> filters = Lists.newArrayList();
			filters.add(userFilter);
			filters.add(metricMeta.getDruidFilter());
			finalFilter = new AndFilter(filters);
		} else if(userFilter != null) {
			finalFilter = userFilter;
		} else {
			finalFilter = metricMeta.getDruidFilter();
		}

		List<BaseAggregator> aggregations = metricMeta.getDruidAggregators();
		if (aggregations == null) {
			throw new SqlTranslationException(ExceptionErrorCode.INVALID_METRIC.getErrorMessage() + metricProps.getMetric());
		}
		
		List<BasePostAggregator> postAggregations = metricMeta.getDruidPostAggregators();
				
		Set<String> metricNames = Sets.newHashSet();
		Map<String, String> aggregateNames = Maps.newHashMap();
		for(BaseAggregator a : aggregations) {
			metricNames.add(a.getName());
			if(a instanceof HyperUniqueAggregator){
				aggregateNames.put(a.getName(), ((HyperUniqueAggregator)a).getFieldName());
			}else{
				aggregateNames.put(a.getName(), a.getName());
			}
		}
		if(postAggregations != null && postAggregations.size() > 0) {
			for(BasePostAggregator p : postAggregations) {
				metricNames.add(p.getName());
			}
		}
		
		// Sort by
		String sort = req.getSort();
		if(sort == null){
			sort = metricProps.getMetric();
		}else{
			sort = sort.toLowerCase();
		}
		if(!metricNames.contains(sort)) {
			throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_SORT_PARAM.getErrorMessage() + sort);
		}

		BaseHaving finalHaving = null;
		if(req.getDimensions() != null && req.getDimensions().size() > 0){
			BaseHaving userHaving = null;
			if (req.getHaving() != null) {
				if(postAggregations == null)
					postAggregations = Lists.newArrayList();
				userHaving = druidHavingParser.parseHaving(req.getHaving(), aggregateNames, tableMeta, postAggregations);
				if(userHaving == null) {
					throw new SqlTranslationException(ExceptionErrorCode.INVALID_HAVING_CLAUSE.getErrorMessage() + req.getHaving());
				}
			}
	
			if(userHaving != null && metricMeta.getDruidHaving() != null) {
				List<BaseHaving> havings = Lists.newArrayList();
				havings.add(userHaving);
				havings.add(metricMeta.getDruidHaving());
				finalHaving = new AndHaving(havings);
			} else if(userHaving != null) {
				finalHaving = userHaving;
			} else {
				finalHaving = metricMeta.getDruidHaving();
			}
		}
		
		DruidSpecs druidSpecs = null;
		if(req.getDimensions() != null){
			//Real Dimension Name to Alias map
			final Map<String, String> nameAliasMap = Maps.newHashMap();
			List<String> transformedDimensions = FluentIterable
					.from(req.getDimensions())
					.transform(new Function<String, String>() {
						@Override
						public String apply(String input) {
							PulsarTableDimension pulsarTableDimMeta = (PulsarTableDimension)tableMeta.getDimensionByName(input);
							if(pulsarTableDimMeta != null && pulsarTableDimMeta.getRTOLAPColumnName() != null){
								if(!pulsarTableDimMeta.getRTOLAPColumnName().equals(input)){
									nameAliasMap.put(pulsarTableDimMeta.getRTOLAPColumnName(), input);
									return pulsarTableDimMeta.getRTOLAPColumnName();
								}
							}
							return input;
						}
					}).toList();
			// Set All Druid Parameters for queryDruid()
			druidSpecs = new DruidSpecs(tableName, transformedDimensions, nameAliasMap, aggregations, postAggregations);
		}else{
			druidSpecs = new DruidSpecs(tableName, null, null, aggregations, postAggregations);
		}
		
		int maxResults = getMaxResults (req, false);
		
		druidSpecs.setFilter(finalFilter)
		.setSort(sort)
		.setHaving(finalHaving)
		.setLimit(maxResults)
		.setTableColumnsMeta(tableMeta)
		.setIntervals(metricProps.getIntevals())
		.setGranularity(granularity);
		
		DataSourceProvider pulsarDB = getPulsarDB();
		DruidQueryParameter druidQueryParam = new DruidQueryParameter(druidSpecs, req.getNamespace());
		return druidQueryProcessor.queryDruid(druidQueryParam, pulsarDB.getConnector());
	}

	
	@Override
	public TraceAbleResponse doSQLQuery(SQLQueryContext queryContext)
			throws JsonParseException, JsonMappingException, IOException,
			ParseException,DataSourceConfigurationException,DataSourceException,
			SqlTranslationException,InvalidQueryParameterException {
		DataSourceProvider pulsarDB = getPulsarDB();
		Table table  = pulsarDB.getTableByName(queryContext.getTableNames().get(0));
		if (table == null) {
			throw new SqlTranslationException(
					ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage() + "Invalid table name:" + queryContext.getTableNames().get(0));
		}
		
		PulsarTable pulsarTable = (PulsarTable) table;
		List<String> tableNames = Lists.newArrayList();

		tableNames.add(pulsarTable.getRTOLAPTableName());
		queryContext.setTableNames(tableNames);
		return druidQueryProcessor.doSQLQuery(queryContext);
	}
	
	private int getMaxResults (BaseRequest req, boolean kylinSource) {
		int maxResults = SQLTranslator.DEFAULT_LIMIT;
		if(req.getMaxResults() != null && req.getMaxResults() > 0) {
			maxResults = req.getMaxResults();
			return maxResults;
		}

		// Default Limit when no maxResults is in the Request
		List<String> dimensions = req.getDimensions();
		if (dimensions == null || dimensions.size() == 0) {
			// TopN or GroupBy are no Default to 10
			if (kylinSource) {
				// TimeSeries Kylin set to 1000
				maxResults = 1000;
			}
		}
		return maxResults;
	}
	
	private DataSourceProvider getPulsarDB() {
		return DataSourceMetaRepo.getInstance().getDBMetaFromCache(DataSourceConfigurationLoader.PULSAR_DATASOURCE);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		restValidator = new PulsarRestValidator(pulsarRestMetricRegistry);
		druidQueryProcessor = new DruidQueryProcessor();	
		druidQueryProcessor.setResultEnrichers(getResultEnrichers());
		druidQueryProcessor.setCacheProvider(getCacheProvider());
	}

	@Override
	public SQLTranslator getSqlTranslator() {
		return druidQueryProcessor.getSqlTranslator();
	}
}
