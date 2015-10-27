/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.query;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.query.request.BaseSQLRequest;
import com.ebay.pulsar.analytics.query.request.DateRange;
import com.ebay.pulsar.analytics.query.request.PulsarDateTimeFormatter;
import com.ebay.pulsar.analytics.query.request.SQLRequest;
import com.ebay.pulsar.analytics.query.response.TraceAbleResponse;
import com.ebay.pulsar.analytics.query.result.ResultEnricher;
import com.ebay.pulsar.analytics.query.sql.SQLTranslator;
import com.ebay.pulsar.analytics.query.validator.QueryValidator;
import com.ebay.pulsar.analytics.query.validator.SQLQueryValidator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Lists;

/**
 * 
 * @author mingmwang
 *
 */
public abstract class AbstractSQLQueryProcessor implements SQLQueryProcessor {
	private static final Logger logger = LoggerFactory.getLogger(AbstractSQLQueryProcessor.class);
	
	private QueryValidator<BaseSQLRequest> sqlRequestValidator = new SQLQueryValidator() ;
	private Map<String, ResultEnricher> resultEnrichers;
		
	public Map<String, ResultEnricher> getResultEnrichers() {
		return resultEnrichers;
	}

	public void setResultEnrichers(Map<String, ResultEnricher> resultEnrichers) {
		this.resultEnrichers = resultEnrichers;
	}
	
	public SQLQueryContext prepareQuery(BaseSQLRequest req, String dataSourceName) throws SqlTranslationException{
		sqlRequestValidator.validate(req);
		DateRange intervals = getDataRangeFromSqlRequest(req);
		
		String tableNameWithNS = getSqlTranslator().getTableName(req.getSql());
		String tableName = null;
		
		int idx = tableNameWithNS.lastIndexOf('.');
		if(idx > 0){
			tableName = tableNameWithNS.substring(idx+1);
		}else{
			tableName = tableNameWithNS;
		}
				
		SQLQueryContext context = new SQLQueryContext();
		context.setGranularity(req.getGranularity());
		context.setIntervals(intervals);
		context.setSqlQuery(req.getSql());
		context.setNs(req.getNamespace());
		
		List<String> tableNames = Lists.newArrayList();
		tableNames.add(tableName);
		context.setTableNames(tableNames);
	
		List<String> dbNameSpaces = Lists.newArrayList();
		dbNameSpaces.add(dataSourceName);
		context.setDbNameSpaces(dbNameSpaces);
		return context;
	}

	@PreAuthorize("(#dataSourceName.equals('pulsarholap')) or hasAuthority(#dataSourceName+'_MANAGE') or hasAuthority(#dataSourceName+'_VIEW') or hasAuthority('SYS_MANAGE_DATASOURCE') or hasAuthority('SYS_VIEW_DATASOURCE')")
	public TraceAbleResponse executeQuery(BaseSQLRequest req, String dataSourceName) throws JsonParseException, JsonMappingException, IOException, ParseException {
		SQLQueryContext queryContext = prepareQuery(req, dataSourceName);
		return doSQLQuery(queryContext);
	}
	
	public abstract SQLTranslator getSqlTranslator();
	

	public abstract TraceAbleResponse doSQLQuery(SQLQueryContext queryContext) throws JsonParseException, JsonMappingException, IOException, ParseException;

	private DateRange getDataRangeFromSqlRequest(BaseSQLRequest req) {
		SQLRequest sqlReq = (SQLRequest) req;
		DateTime start = null;
		DateTime end = null;

		String intervalStr = sqlReq.getIntervals();
		String startTime = sqlReq.getStartTime();
		String endTime = sqlReq.getEndTime();

		if (intervalStr != null) {
			String[] strArr = intervalStr.split("/");
			if (strArr.length == 2) {
				startTime = strArr[0];
				endTime = strArr[1];
			}
		}
		
		try {
			start = PulsarDateTimeFormatter.INPUTTIME_FORMATTER.parseDateTime(startTime);
			end = PulsarDateTimeFormatter.INPUTTIME_FORMATTER.parseDateTime(endTime);
		} catch (Exception e) {
			logger.warn ("DataFormatException:"+ e.getMessage());
		}
		if(start == null || end == null)
			return null;
		
		DateTime now = new DateTime();
		// If end time is later than current time, set it to current time.
		// Otherwise a partial result will be cached, and later queries will return the same partial result
		if(end.compareTo(now) > 0) {
		end = now;
		}
		DateRange intervals = new DateRange(start, end);
		return intervals;
	}
}
