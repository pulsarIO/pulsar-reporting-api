/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.metricstore.druid.query.sql;

import java.util.Map;

import com.ebay.pulsar.analytics.datasource.PulsarTableDimension;
import com.ebay.pulsar.analytics.datasource.Table;
import com.ebay.pulsar.analytics.datasource.TableDimension;
import com.ebay.pulsar.analytics.query.sql.SQLTranslator;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.QueryTreeNode;

/**
 * 
 * @author mingmwang
 *
 */
public abstract class AbstractDruidSqlTranslator extends SQLTranslator {
	@Override
	public String checkNameChange (ColumnReference column, Table tableColumnsMeta, Map<Integer, QueryTreeNode> nodesMapIn) {
		String colNameDruid = null;
		TableDimension tableDimension = tableColumnsMeta.getColumnMeta(column.getColumnName());
		if (tableDimension != null && (tableDimension instanceof PulsarTableDimension)) {
			colNameDruid = ((PulsarTableDimension) tableDimension).getRTOLAPColumnName();
			if(colNameDruid != null)
				return colNameDruid;
		}
		return column.getColumnName();
	}
}
