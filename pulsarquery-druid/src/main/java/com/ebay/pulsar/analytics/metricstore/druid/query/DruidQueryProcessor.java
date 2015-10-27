/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.ebay.pulsar.analytics.cache.Cache;
import com.ebay.pulsar.analytics.cache.CacheConfig;
import com.ebay.pulsar.analytics.cache.CacheProvider;
import com.ebay.pulsar.analytics.constants.Constants.RequestNameSpace;
import com.ebay.pulsar.analytics.datasource.DBConnector;
import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.DataSourceProvider;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.DataSourceConfigurationException;
import com.ebay.pulsar.analytics.exception.DataSourceException;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.BaseAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.aggregator.HyperUniqueAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.constants.Constants.SortDirection;
import com.ebay.pulsar.analytics.metricstore.druid.filter.AndFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.OrFilter;
import com.ebay.pulsar.analytics.metricstore.druid.filter.SelectorFilter;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.BaseGranularity;
import com.ebay.pulsar.analytics.metricstore.druid.granularity.DruidGranularityParser;
import com.ebay.pulsar.analytics.metricstore.druid.having.BaseHaving;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.DefaultLimitSpec;
import com.ebay.pulsar.analytics.metricstore.druid.limitspec.OrderByColumnSpec;
import com.ebay.pulsar.analytics.metricstore.druid.metric.NumericMetric;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.BaseQuery;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.DruidSpecs;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.GroupByQuery;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.TimeSeriesQuery;
import com.ebay.pulsar.analytics.metricstore.druid.query.model.TopNQuery;
import com.ebay.pulsar.analytics.metricstore.druid.query.sql.DruidSQLTranslator;
import com.ebay.pulsar.analytics.metricstore.druid.query.validator.DruidGranularityValidator;
import com.ebay.pulsar.analytics.metricstore.druid.query.validator.GranularityAndTimeRange;
import com.ebay.pulsar.analytics.query.AbstractSQLQueryProcessor;
import com.ebay.pulsar.analytics.query.SQLQueryContext;
import com.ebay.pulsar.analytics.query.request.PulsarDateTimeFormatter;
import com.ebay.pulsar.analytics.query.response.TraceAbleResponse;
import com.ebay.pulsar.analytics.query.response.TraceQuery;
import com.ebay.pulsar.analytics.query.result.ChainedRevisor;
import com.ebay.pulsar.analytics.query.result.ColumnNameRevisor;
import com.ebay.pulsar.analytics.query.result.ColumnValueCollector;
import com.ebay.pulsar.analytics.query.result.HllMetricRevisor;
import com.ebay.pulsar.analytics.query.result.ResultEnricher;
import com.ebay.pulsar.analytics.query.result.ResultNode;
import com.ebay.pulsar.analytics.query.result.ResultRevisor;
import com.ebay.pulsar.analytics.query.validator.QueryValidator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 * @author mingmwang
 *
 */
public class DruidQueryProcessor extends AbstractSQLQueryProcessor {
	private static final int TOPN_EXPAND_LIMIT = 10;
	private static final double EXPAND_RATE = 1.3;
	private static final int TOPN_MAX = 10000;
	private static final String TIMESTAMP = "timestamp";
	private static final String RESULT = "result";

	private QueryValidator<GranularityAndTimeRange> druidValidator = new DruidGranularityValidator();
	private DruidSQLTranslator sqlTranslator = new DruidSQLTranslator();

	private CacheProvider cacheProvider;
	
	public CacheProvider getCacheProvider() {
		return cacheProvider;
	}

	public void setCacheProvider(CacheProvider cacheProvider) {
		this.cacheProvider = cacheProvider;
	}

	@Override
	public TraceAbleResponse doSQLQuery(SQLQueryContext queryContext)
			throws JsonParseException, JsonMappingException, IOException,
			ParseException, DataSourceConfigurationException,DataSourceException,
			SqlTranslationException,InvalidQueryParameterException {
		BaseGranularity granularity = DruidGranularityParser.parse(queryContext.getGranularity());
		druidValidator.validate(new GranularityAndTimeRange(granularity, queryContext.getIntervals()));
			
		DataSourceProvider dbInstance = DataSourceMetaRepo.getInstance()
				.getDBMetaFromCache(queryContext.getDbNameSpaces().get(0));
		if (dbInstance == null) {
			throw new InvalidQueryParameterException(ExceptionErrorCode.DATASOURCE_ERROR.getErrorMessage()+
					"Invalid DB query namespace:"
							+ queryContext.getDbNameSpaces().get(0));
		}
		DruidSpecs druidSpec = getSqlTranslator().parseSql(queryContext.getSqlQuery(), dbInstance, queryContext.getTableNames().get(0));
		if (druidSpec == null) {
			throw new SqlTranslationException(
					ExceptionErrorCode.MISSING_SQL.getErrorMessage()
							+ queryContext.getSqlQuery());
		}

		List<BaseAggregator> aggregators = druidSpec.getAggregators();
		if (aggregators == null || aggregators.size() == 0) {
			throw new SqlTranslationException(
					ExceptionErrorCode.SQL_PARSING_ERROR.getErrorMessage()
							+ queryContext.getSqlQuery());
		}
		druidSpec.setGranularity(granularity).
		setIntervals(queryContext.getIntervals());
		
		return queryDruid(new DruidQueryParameter(druidSpec, queryContext.getNs(), queryContext.getDbNameSpaces()), dbInstance.getConnector());
	}

	public TraceAbleResponse queryDruid(DruidQueryParameter parameterObject, DBConnector connector) throws IOException, JsonParseException,
			JsonMappingException, ParseException,DataSourceConfigurationException,DataSourceException,
			SqlTranslationException,InvalidQueryParameterException {

		BaseQuery query;
	
		// Get All Druid Parameters from DruidSpecs
		DruidSpecs druidSpec = parameterObject.getDruidSpecs();
		List<String> strIntervals = PulsarDateTimeFormatter
				.buildStringIntervals(druidSpec.getIntervals());
		
		String dataSource = druidSpec.getFromTable();
		List<String> dimensions = druidSpec.getDimensions();
		BaseFilter filter = druidSpec.getFilter();
		BaseHaving having = druidSpec.getHaving();
		List<BaseAggregator> aggregations = druidSpec.getAggregators();
		List<BasePostAggregator> postAggregations = druidSpec
				.getPostAggregators();
		Table tableMeta = druidSpec.getTableColumnsMeta();

		int maxResults = druidSpec.getLimit();
		DefaultLimitSpec limitSpec = druidSpec.getLimitSpec();

		boolean useGroupBy4TopN = false;
		if (dimensions != null && dimensions.size() == 1) {
			String topNdim = dimensions.get(0);
			if (tableMeta.getDimensionByName(topNdim).isMultiValue()) {
				useGroupBy4TopN = true;
			}
			if(having != null){
				useGroupBy4TopN = true;
			}
			
			if(limitSpec != null){
				List<OrderByColumnSpec> orderByColumns = limitSpec.getColumns();
				if(orderByColumns != null && orderByColumns.size() > 1){
					useGroupBy4TopN = true;
				}else if(orderByColumns != null && orderByColumns.size() == 1){
					OrderByColumnSpec orderBySpec = orderByColumns.get(0);
					if(orderBySpec.getDirection() == SortDirection.ascending){
						useGroupBy4TopN = true;
					}
				}
			}
		}

		boolean topNRevise = false;
		if (dimensions == null || dimensions.size() == 0) {
			query = new TimeSeriesQuery(dataSource, strIntervals,
						druidSpec.getGranularity(),
						aggregations);
		} else if (dimensions.size() == 1 && !useGroupBy4TopN) {
			if(!BaseGranularity.ALL.equals(druidSpec.getGranularity())){
				topNRevise = true;
				// Expand limit
				if (maxResults > TOPN_EXPAND_LIMIT) {
					maxResults = (int) Math.floor(maxResults * EXPAND_RATE);
				}
			}
			query = new TopNQuery(dataSource, strIntervals,
					druidSpec.getGranularity(),
					aggregations, dimensions.get(0), maxResults,
					new NumericMetric(druidSpec.getSort()));
			
		} else {
			DefaultLimitSpec defaultLimitSpec = null;
			if (limitSpec != null) {
				defaultLimitSpec = limitSpec;
			} else {
				OrderByColumnSpec orderByColumnSpec = new OrderByColumnSpec(
						druidSpec.getSort(), SortDirection.descending);
				List<OrderByColumnSpec> columns = Lists.newArrayList();
				columns.add(orderByColumnSpec);
				defaultLimitSpec = new DefaultLimitSpec(maxResults, columns);
			}
			GroupByQuery groupByQuery = new GroupByQuery(dataSource, strIntervals,druidSpec.getGranularity(),
						aggregations, dimensions);

			if (having != null) {
				groupByQuery.setHaving(having);
			}
			groupByQuery.setLimitSpec(defaultLimitSpec);
			query = groupByQuery;
		}
		TraceQuery trace = new TraceQuery();
		trace.setQuery(query);

		TraceAbleResponse tresp = new TraceAbleResponse();
		tresp.setQuery(trace);

		query.setFilter(filter);
		if (postAggregations != null) {
			query.setPostAggregations(postAggregations);
		}

		StringBuilder catchKeyNSBuilder = new StringBuilder();
		Joiner.on('_')
				.skipNulls()
				.appendTo(catchKeyNSBuilder, parameterObject.getNs(),
						parameterObject.getDbNameSpaces(),
						query.getQueryType().name());
		String catchkeyNS = catchKeyNSBuilder.toString();
		try {
			if (topNRevise) {
				if (maxResults <= TOPN_EXPAND_LIMIT) {
					String druidRsp = reviseTopNWithTwoQueries(parameterObject,
							connector, query, trace, catchkeyNS);
					tresp.setQueryResult(postQuery(druidRsp, druidSpec,
							useGroupBy4TopN, false));
				} else {
					String druidRsp = getQueryResultsFromCache(query, trace,
							connector, catchkeyNS, parameterObject.getNs());
					tresp.setQueryResult(postQuery(druidRsp, druidSpec,
							useGroupBy4TopN, true));
				}
			} else {
				String druidRsp = getQueryResultsFromCache(query, trace,
						connector, catchkeyNS, parameterObject.getNs());
				tresp.setQueryResult(postQuery(druidRsp, druidSpec,
						useGroupBy4TopN, false));
			}
		} catch (SocketTimeoutException ex) {
			List<Map<String, Object>> result = Lists.newArrayList();
			Map<String, Object> errorMap = Maps.newHashMap();
			errorMap.put("Error", ex.getMessage());
			result.add(errorMap);
			tresp.setQueryResult(result);
		}
		return tresp;
	}

	private String reviseTopNWithTwoQueries(
			DruidQueryParameter parameterObject, DBConnector connector,
			BaseQuery query, TraceQuery trace, String catchkeyNS)
			throws IOException, JsonParseException, JsonMappingException,
			ParseException,DataSourceConfigurationException,DataSourceException,
			SqlTranslationException,InvalidQueryParameterException {
		// First time query with 'All' Granularity.
		query.setGranularity(BaseGranularity.ALL);
		String druidRsp = getQueryResultsFromCache(query, trace, connector,
				catchkeyNS, parameterObject.getNs());

		// Second time query
		String dimension = parameterObject.getDruidSpecs().getDimensions()
				.get(0);
		reviseDruidTopNQueryFilter(druidRsp, query, dimension);
		query.setGranularity(parameterObject.getDruidSpecs().getGranularity());
		druidRsp = getQueryResultsFromCache(query, trace, connector,
				catchkeyNS, parameterObject.getNs());
		return druidRsp;
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> postQuery(String druidRsp,
			DruidSpecs druidSpecs, boolean groupBy4TopN,
			boolean expandTopN) throws IOException, JsonParseException,
			JsonMappingException, ParseException,DataSourceConfigurationException,DataSourceException,
			SqlTranslationException,InvalidQueryParameterException {
		List<String> dimensions = druidSpecs.getDimensions();
		Set<String> hllSet = getAggregateHLL(druidSpecs.getAggregators());

		ObjectMapper mapper = new ObjectMapper();
		List<Map<String, Object>> listOfResult = mapper.readValue(druidRsp,
				new TypeReference<List<Map<String, Object>>>() {
				});

		List<ColumnValueCollector<String>> valueCollectorList = Lists
				.newArrayList();
		if (getResultEnrichers() != null) {
			for (Map.Entry<String, ResultEnricher> entry : getResultEnrichers()
					.entrySet()) {
				valueCollectorList.add(new ColumnValueCollector<String>(entry
						.getKey(), Sets.<String> newHashSet()));
			}
		}

		List<Map<String, Object>> rsp = Lists.newArrayList();
		List<String> topNDimensions = null;
		if (expandTopN) {
			topNDimensions = resortTopN(druidSpecs,
					dimensions.get(0), listOfResult);
		}

		for (Map<String, Object> resultMap : listOfResult) {
			DateTime dt = PulsarDateTimeFormatter.OUTPUT_DRUID_TIME_FORMATTER
					.parseDateTime(resultMap.get(TIMESTAMP).toString());
			DateTime start = druidSpecs.getIntervals().getStart();
			int compareDate = DateTimeComparator.getInstance().compare(dt,
					start);
			if (compareDate < 0) {
				dt = start;
			}
			String dtMST = PulsarDateTimeFormatter.OUTPUTTIME_FORMATTER
					.print(dt);
			resultMap.put(TIMESTAMP, dtMST);

			// Reformat druid group by query result
			if (dimensions != null && (dimensions.size() > 1 || groupBy4TopN)) {
				resultMap.put(RESULT, resultMap.get("event"));
				resultMap.remove("event");
				resultMap.remove("version");
			}

			// Reformat Non-topN queries
			if (dimensions == null || dimensions.size() == 0
					|| dimensions.size() > 1 || groupBy4TopN) {
				Map<String, Object> result = (Map<String, Object>) resultMap
						.get(RESULT);
				Map<String, Object> reviseResult = reviseResult(dimensions,
						druidSpecs.getNameAliasMap(), hllSet,
						valueCollectorList, result);
				resultMap.put(RESULT, reviseResult);
			} else {
				// //Reformat real topN queries
				List<Map<String, Object>> resultList = (List<Map<String, Object>>) resultMap
						.get(RESULT);
				if (resultList == null || resultList.size() == 0) {
					Map<String, Object> newResultMap = Maps.newHashMap();
					newResultMap.put(TIMESTAMP, dtMST);
					newResultMap.put(RESULT, ImmutableMap.of());
					rsp.add(newResultMap);
				} else {
					for (Map<String, Object> result : resultList) {
						if (topNDimensions != null
								&& !topNDimensions.contains( Strings.nullToEmpty((String)result
										.get(dimensions.get(0))))) {
							// Only the real TopN dimension is kept
							continue;
						}

						Map<String, Object> reviseResult = reviseResult(
								dimensions, druidSpecs.getNameAliasMap(),
								hllSet, valueCollectorList, result);
						Map<String, Object> newResultMap = Maps.newHashMap();
						newResultMap.put(TIMESTAMP, dtMST);
						newResultMap.put(RESULT, reviseResult);
						rsp.add(newResultMap);
					}
				}
			}
		}

		if (dimensions != null && dimensions.size() == 1 && !groupBy4TopN)
			listOfResult = rsp;

		for (ColumnValueCollector<String> columnValueCollector : valueCollectorList) {
			if (getResultEnrichers() != null) {
				if (!columnValueCollector.getValueCollection().isEmpty()) {
					Map<String, ResultNode> enrichedResult = getResultEnrichers()
							.get(columnValueCollector.getColumnName()).enrich(
									columnValueCollector.getValueCollection());
					for (Map<String, Object> resultMap : listOfResult) {
						if (resultMap.get(RESULT) != null) {
							Map<String, Object> result = (Map<String, Object>) resultMap
									.get(RESULT);
							if (result
									.get(columnValueCollector.getColumnName()) != null) {
								ResultNode resultNode = (ResultNode) enrichedResult
										.get(result.get(columnValueCollector
												.getColumnName()));
								result.put(resultNode.getName(),
										resultNode.getValue());
							}
						}
					}
				}
			}
		}
		return listOfResult;
	}

	private List<String> resortTopN(DruidSpecs druidSpecs, String dimension,
			List<Map<String, Object>> listOfResult) {
		StreamSummary<String> topN = new StreamSummary<String>(TOPN_MAX);
		for (Map<String, Object> map : listOfResult) {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> resultList = (List<Map<String, Object>>) map
					.get(RESULT);
			if (resultList != null) {
				for (Map<String, Object> result : resultList) {
					Number metricNum = (Number) result.get(druidSpecs.getSort());
					;
					String dimStr = (String) result.get(dimension);
					if (dimStr == null) {
						dimStr = "";
					}
					topN.offer(dimStr, metricNum.intValue());
				}
			}
		}
		return FluentIterable.from(topN.topK(druidSpecs.getLimit()))
				.transform(new Function<Counter<String>, String>() {
					@Override
					public String apply(Counter<String> input) {
						return input.getItem();
					}
				}).toList();
	}

	private Map<String, Object> reviseResult(List<String> dimensions,
			Map<String, String> nameAliasMap, Set<String> hllSet,
			List<ColumnValueCollector<String>> valueCollectorList,
			Map<String, Object> result) {
		List<ResultNode> resultNodeList = Lists.newArrayList(Iterables
				.transform(result.entrySet(),
						new Function<Map.Entry<String, Object>, ResultNode>() {
							@Override
							public ResultNode apply(
									final Map.Entry<String, Object> input) {
								return new ResultNode(input.getKey(), input
										.getValue());
							}
						}));
		List<ResultRevisor> revisorList = Lists.newArrayList();
		revisorList.add(new HllMetricRevisor(hllSet));
		revisorList.add(new ColumnNameRevisor(dimensions, nameAliasMap));
		revisorList.addAll(valueCollectorList);

		ChainedRevisor chainedRevisor = new ChainedRevisor(revisorList);
		for (ResultNode resultNode : resultNodeList) {
			resultNode.revise(chainedRevisor);
		}
		Map<String, Object> reviseResult = Maps.newLinkedHashMap();
		for (ResultNode resultNode : resultNodeList) {
			reviseResult.put(resultNode.getName(), resultNode.getValue());
		}
		return reviseResult;
	}

	private Set<String> getAggregateHLL(List<BaseAggregator> aggregators) {
		Set<String> hllSet = Sets.newHashSet();
		for (BaseAggregator aggregator : aggregators) {
			if (aggregator instanceof HyperUniqueAggregator) {
				HyperUniqueAggregator hllAggregator = (HyperUniqueAggregator) aggregator;
				String name = hllAggregator.getName();
				hllSet.add(name.toLowerCase());
			}
		}
		return hllSet;
	}

	private void reviseDruidTopNQueryFilter(String druidRsp, BaseQuery query,
			String dimension) throws IOException, JsonParseException,
			JsonMappingException, ParseException {
		BaseFilter originalfilter = query.getFilter();
		List<Map<String, Object>> list = Lists.newArrayList();
		List<String> dimensionList = Lists.newArrayList();

		ObjectMapper mapper = new ObjectMapper();
		list = mapper.readValue(druidRsp,
				new TypeReference<List<Map<String, Object>>>() {
				});

		for (Map<String, Object> map : list) {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> resultList = (List<Map<String, Object>>) map
					.get(RESULT);
			if (resultList == null || resultList.size() == 0) {
				continue;
			} else {
				for (Map<String, Object> resultMap : resultList) {
					dimensionList.add((String)resultMap.get(dimension));
				}
			}
		}
		if (dimensionList.size() != 0) {
			List<BaseFilter> filterList = Lists.newArrayList();
			for (String dimTopN : dimensionList) {
				SelectorFilter dimFilter = new SelectorFilter(dimension,
						dimTopN);
				filterList.add(dimFilter);
			}
			OrFilter orFilter = new OrFilter(filterList);
			List<BaseFilter> filterList2 = Lists.newArrayList();
			if (originalfilter != null) {
				filterList2.add(originalfilter);
				filterList2.add(orFilter);
				AndFilter andFilter = new AndFilter(filterList2);
				query.setFilter(andFilter);
			} else {
				query.setFilter(orFilter);
			}
		}
	}

	private String getQueryResultsFromCache(BaseQuery query, TraceQuery trace,
			DBConnector connector, String catchKeyNS, RequestNameSpace ns) {
		boolean useCache = false;
		boolean populateCache = false;
		Cache cache = null;
		CacheConfig config = null;
		Cache.NamedKey key = null;
		if (cacheProvider != null) {
			config = cacheProvider.getCacheConfig();
			useCache = config.isUseCache()
					&& config.isQueryCacheable(query.getQueryType().name());
			populateCache = config.isPopulateCache()
					&& config.isQueryCacheable(query.getQueryType().name());
			if (useCache || populateCache) {
				cache = cacheProvider.get();

				key = new Cache.NamedKey(catchKeyNS, query.cacheKey());
				trace.setCachekey(key.toByteArray());
			} else {
				cache = null;
				key = null;
			}

			if (useCache) {
				long start = System.nanoTime();
				byte[] cachedResult = cache.get(key);
				if (cachedResult != null) {
					trace.setFromcache(true);
					trace.setCachegettime(System.nanoTime() - start);
					trace.setBytesize(cachedResult.length);
					return new String(cachedResult);
				}
			}
		}

		long start = System.nanoTime();
		String druidRsp = (String) connector.query(query);
		trace.setDruidquerytime(System.nanoTime() - start);

		if (populateCache) {
			cache.put(key, druidRsp.getBytes(), config.getExpiration(ns.name()));
			trace.setTocache(true);
		}
		return druidRsp;
	}

	@Override
	public DruidSQLTranslator getSqlTranslator() {
		return sqlTranslator;
	}
}
