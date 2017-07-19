package org.cloudgraph.hbase.io;

import org.cloudgraph.state.GraphRow;

import commonj.sdo.DataObject;

public class DefaultRowOperation extends GraphRow implements RowOperation {


	public DefaultRowOperation(byte[] rowKey, DataObject rootDataObject) {
		super(rowKey, rootDataObject);
	}

}
