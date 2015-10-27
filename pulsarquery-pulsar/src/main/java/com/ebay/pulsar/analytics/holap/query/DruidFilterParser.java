/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.holap.query;

import java.util.List;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.metricstore.druid.filter.BaseFilter;
import com.ebay.pulsar.analytics.metricstore.druid.query.sql.DruidFilterTranslator;
import com.ebay.pulsar.analytics.query.sql.FilterHavingParser;
import com.foundationdb.sql.parser.ValueNode;

////////////////////////////////////////////////////////////
// The DruidFilterParser can be used as the following:
//
// DruidFilterParser parser = new DruidFilterParser();
// BaseFilter userFilter = parser.parseWhere(req.getFilter(), tableMeta);
////////////////////////////////////////////////////////////

public class DruidFilterParser extends FilterHavingParser{
	private DruidFilterTranslator filterTranslator = new DruidFilterTranslator();

	public BaseFilter parseWhere(String whereClause, Table tableMeta) throws SqlTranslationException {
		// Construct SQL Statement "SELECT * FROM TBL WHERE "
		StringBuilder strBuilder = new StringBuilder (SELECT_FROM_WHERE);
		strBuilder.append(whereClause);

		BaseFilter filter = null;
		QueryWhereHaving queryDesc = parse(strBuilder.toString(), tableMeta);
		List<ValueNode> vNodes = queryDesc.getWhereClauses();
		if (vNodes != null && !vNodes.isEmpty()) {
			// Should only have 1 node
			ValueNode vNode = vNodes.get(0);
			filter = filterTranslator.valueNodeToFilter(vNode, tableMeta);
		}
		return filter;
	}
}
