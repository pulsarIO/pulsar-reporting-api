/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.holap.query.validator;

import java.util.List;
import java.util.Set;

import com.ebay.pulsar.analytics.datasource.DataSourceMetaRepo;
import com.ebay.pulsar.analytics.datasource.DataSourceProvider;
import com.ebay.pulsar.analytics.datasource.PulsarRestMetricMeta;
import com.ebay.pulsar.analytics.datasource.PulsarRestMetricRegistry;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.loader.DataSourceConfigurationLoader;
import com.ebay.pulsar.analytics.exception.ExceptionErrorCode;
import com.ebay.pulsar.analytics.exception.InvalidQueryParameterException;
import com.ebay.pulsar.analytics.query.request.BaseRequest;
import com.ebay.pulsar.analytics.query.request.CoreRequest;
import com.ebay.pulsar.analytics.query.request.RealtimeRequest;
import com.ebay.pulsar.analytics.query.validator.QueryValidator;

/**
 * 
 * @author mingmwang
 *
 */
public class PulsarRestValidator implements QueryValidator<BaseRequest> {
	private static String ENDPOINT_REALTIME = "realtime";
	private static String ENDPOINT_CORE = "core";
	
	private PulsarRestMetricRegistry pulsarRestMetricRegistry;

	public PulsarRestValidator(PulsarRestMetricRegistry pulsarRestMetricRegistry){
		this.pulsarRestMetricRegistry = pulsarRestMetricRegistry;
	}
	
	@Override
	public void validate(BaseRequest req){
		List<String> metricList = req.getMetrics();
		if(metricList == null) {
			throw new InvalidQueryParameterException(ExceptionErrorCode.MISSING_METRIC.getErrorMessage());
		} else if(metricList.isEmpty()) {
				throw new InvalidQueryParameterException(ExceptionErrorCode.MISSING_METRIC.getErrorMessage());
		} else if(req.getMetrics().size() > 1) {
			throw new InvalidQueryParameterException(ExceptionErrorCode.MULTI_METRICS_ERROR.getErrorMessage());
		}

		PulsarRestMetricMeta metricMeta = null;
		String metricName = null;
		for(String metric : metricList) {
			// Actually just ONE metric now
			metricName = metric.toLowerCase();
			metricMeta = pulsarRestMetricRegistry.getMetricsMetaFromName(metricName);
			if (metricMeta == null) {
				throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_METRIC.getErrorMessage() + metricName);
			}
			Set<String> metricEndpointSet = metricMeta.getMetricEndpoints();
			if (metricEndpointSet == null) {
				throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_ENDPOINTS.getErrorMessage() + metricName);
			}
			if(req instanceof RealtimeRequest) {
				if (!metricEndpointSet.contains(ENDPOINT_REALTIME)) {
					throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_ENDPOINTS.getErrorMessage() + metricName);
				}
			} else if(req instanceof CoreRequest) {
				if (!metricEndpointSet.contains(ENDPOINT_CORE)) {
					throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_ENDPOINTS.getErrorMessage() + metricName);
				}
			} else {
				// Shouldn't reach here
			}
		}
		
		Integer maxResults = req.getMaxResults(); 
		if(maxResults != null && maxResults <= 0) {
			throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_MAXRESULT.getErrorMessage() + maxResults);
		}

		if(req instanceof RealtimeRequest) {
			Integer duration = ((RealtimeRequest)req).getDuration();
			if(duration != null && (duration <= 0 || duration > 1800)) {
				throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_DURATION.getErrorMessage() + duration);
			}
		}

		String tableName = metricMeta.getTableName();
		if (tableName == null) {
			throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_METRIC.getErrorMessage() + metricName);
		}
		
		DataSourceProvider pulsarDB = DataSourceMetaRepo.getInstance().getDBMetaFromCache(DataSourceConfigurationLoader.PULSAR_DATASOURCE);
		Table pulsarTable = pulsarDB.getTableByName(tableName);
		if (pulsarTable == null) {
			throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_METRIC.getErrorMessage() + metricName);
		}
			
		if(req.getDimensions() != null) {
			for(String dimension : req.getDimensions()) {
				if(pulsarTable.getDimensionByName(dimension) == null){
					throw new InvalidQueryParameterException(ExceptionErrorCode.INVALID_DIMENSION.getErrorMessage() + dimension);
				}
			}
		}
	}
}
