/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.filters;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * 
 * @author rtao
 *
 */
public class ApiOriginFilter implements javax.servlet.Filter {
	@Override
	public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {
		HttpServletResponse res = (HttpServletResponse) response;
		HttpServletRequest req = (HttpServletRequest) request;
		String originHeader = req.getHeader("Origin");
		res.addHeader("Access-Control-Allow-Origin", originHeader);
		res.setHeader("Access-Control-Allow-Credentials", "true");
		res.addHeader("Access-Control-Allow-Headers",
				"Content-Type,Authorization");
		res.addHeader("Access-Control-Allow-Methods",
				"GET, POST, DELETE, PUT,OPTION,OPTIONS");

		if (req.getMethod().equals("OPTION")
				|| req.getMethod().equals("OPTIONS")) {

			res.setHeader("Access-Control-Allow-Origin", originHeader);
			res.addHeader("Access-Control-Allow-Methods",
					"DELETE, GET, OPTIONS, POST,  PUT, UPDATE");
			res.addHeader(
					"Access-Control-Allow-Headers",
					"Authorization, withCredentials,Content-Type, Timeout, X-File-Size, X-Requested-With");
			res.addHeader("Access-Control-Expose-Headers",
					"DAV, content-length, Allow");
			res.addHeader("Access-Control-Max-Age", "86400");
		}
		chain.doFilter(request, response);
	}

	@Override
	public void destroy() {
	}

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
	}
}
