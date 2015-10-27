/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.resources;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.ebay.pulsar.analytics.cache.CacheConfig;
import com.ebay.pulsar.analytics.cache.CacheProvider;
import com.ebay.pulsar.analytics.cache.CacheStats;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

@Component
@Scope("request")
@Path("/cache")
public class CachingResource {
	private static final Logger logger = LoggerFactory.getLogger(CachingResource.class);

	private static CacheProvider cacheProvider;

	public static CacheProvider getCacheProvider() {
		return cacheProvider;
	}

	public static void setCacheProvider(CacheProvider cacheProvider) {
		CachingResource.cacheProvider = cacheProvider;
	}

	@GET
	@Path("/stats")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getStats(@Context HttpServletRequest request) {
		CacheStats stats = cacheProvider.get().getStats();
		String body = "";
		ObjectWriter statswriter = new ObjectMapper().writerWithType(CacheStats.class);
		try {
			body = statswriter.writeValueAsString(stats);
		} catch (IOException e) {
			logger.warn ("error writing CacheStats to string: " + e.getMessage());
			Map<String, String> errorMap = new HashMap<String, String>();
			errorMap.put("error", e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorMap).build();
		}
		return Response.ok(body).build();
	}

	@GET
	@Path("/config")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getConfig(@Context HttpServletRequest request) {
		CacheConfig config = cacheProvider.getCacheConfig();
		String body = "";
		try {
			ObjectWriter statswriter = new ObjectMapper().writerWithType(CacheConfig.class);
			body = statswriter.writeValueAsString(config);
		} catch (IOException e) {
			logger.warn ("error writing CacheConfig to string: " + e.getMessage());
			Map<String, String> errorMap = new HashMap<String, String>();
			errorMap.put("error", e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorMap).build();
		}
		return Response.ok(body).build();
	}
}