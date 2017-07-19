/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.hbase.graph;

import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.io.RowReader;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;

/**
 * Encapsulates minimal recursive graph traversal information for use in concurrent
 * contexts, such as where a traversal is needed but the initialization data must be
 * captured and stored, and a traversal from a given subroot, initiated later.  
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public class Traversal {
	private PlasmaDataObject subroot;
	private long subrootSequence;
	private EdgeReader collection;
	private PlasmaDataObject source;
	private PlasmaProperty sourceProperty;
	private RowReader rowReader;
	private int level;
	private boolean concurrent;
	
	@SuppressWarnings("unused")
	private Traversal() {}
	 
	public Traversal(PlasmaDataObject subroot, long subrootSequence, EdgeReader collection, PlasmaDataObject source,
			PlasmaProperty sourceProperty,
			RowReader rowReader, boolean concurrent, int level) {
		super();
		this.subroot = subroot;
		this.subrootSequence = subrootSequence;
		this.collection = collection;
		this.source = source;
		this.sourceProperty = sourceProperty;
		this.rowReader = rowReader;
		this.concurrent = concurrent;
		this.level = level;
	}
	public PlasmaDataObject getSubroot() {
		return subroot;
	}
	
	public long getSubrootSequence() {
		return subrootSequence;
	}

	public EdgeReader getCollection() {
		return collection;
	}

	public PlasmaDataObject getSource() {
		return source;
	}
	public PlasmaProperty getSourceProperty() {
		return sourceProperty;
	}
	public RowReader getRowReader() {
		return rowReader;
	}
	public int getLevel() {
		return level;
	}

	public boolean isConcurrent() {
		return concurrent;
	}
	
}

