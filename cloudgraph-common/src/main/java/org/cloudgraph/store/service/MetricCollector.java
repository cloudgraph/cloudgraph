package org.cloudgraph.store.service;

import org.plasma.sdo.PlasmaDataGraphVisitor;

import commonj.sdo.DataObject;

public class MetricCollector implements PlasmaDataGraphVisitor {

	private long count = 0;
	private long depth = 0;
	@Override
	public void visit(DataObject target, DataObject source,
			String sourcePropertyName, int level) {
		count++;
		if (level > depth)
			depth = level;

	}
	public long getCount() {
		return count;
	}
	public long getDepth() {
		return depth;
	}
}
