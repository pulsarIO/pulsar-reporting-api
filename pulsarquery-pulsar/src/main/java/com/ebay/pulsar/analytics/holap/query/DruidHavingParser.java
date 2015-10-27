/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.holap.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.exception.SqlTranslationException;
import com.ebay.pulsar.analytics.metricstore.druid.having.BaseHaving;
import com.ebay.pulsar.analytics.metricstore.druid.postaggregator.BasePostAggregator;
import com.ebay.pulsar.analytics.metricstore.druid.query.sql.DruidHavingTranslator;
import com.ebay.pulsar.analytics.query.sql.FilterHavingParser;
import com.foundationdb.sql.parser.ValueNode;


////////////////////////////////////////////////////////////
//The DruidHavingParser can be used as the following:
//
//DruidHavingParser parser = new DruidHavingParser();//
//BaseHaving userHaving = parser.parseHaving(req.getHaving(), tabelMeta, aggregators, postAggregators);
////////////////////////////////////////////////////////////

public class DruidHavingParser extends FilterHavingParser {
	
	private DruidHavingTranslator havingTranslator = new DruidHavingTranslator();
	
	public DruidHavingParser(){
		super.setColumnCheck(false);
	}
	
	public BaseHaving parseHaving (String havingClause, Map<String, String> aggregateNames, Table tableMeta, List<BasePostAggregator> postAggregators) throws SqlTranslationException {
		// Construct SQL Statement "SELECT * FROM xx GROUP BY yy HAVING "
		StringBuilder strBuilder = new StringBuilder (SELECT_FROM_GROUPBY_HAVING);
		strBuilder.append(havingClause);
		
		BaseHaving having = null;
		Set<String> aggregateSet = aggregateNames.keySet();
		QueryWhereHaving queryDesc = parse (strBuilder.toString(), tableMeta, aggregateSet);
		List<ValueNode> vNodes = queryDesc.getHavingClauses();
		if (vNodes != null && !vNodes.isEmpty()) {
			// Should only have 1 node
			ValueNode vNode = vNodes.get(0);
			having = havingTranslator.valueNodeToHaving (vNode, aggregateNames, postAggregators, tableMeta);
		}
		return having;
	}
}
