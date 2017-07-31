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
package org.cloudgraph.config;

import java.nio.charset.Charset;

import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.core.CoreConstants;

import commonj.sdo.DataObject;

/**
 * The configuration for a row or column key.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public abstract class KeyFieldConfig {
	protected int sequenceNum;
	/** the total number of fields in the row or column composite key */
	protected int totalFields;
	protected Charset charset = Charset.forName(CoreConstants.UTF8_ENCODING);
	private KeyField field;

	@SuppressWarnings("unused")
	private KeyFieldConfig() {
	}

	public KeyFieldConfig(KeyField field, int sequenceNum, int totalFields) {
		super();
		this.field = field;
		this.sequenceNum = sequenceNum;
		this.totalFields = totalFields;
	}

	public int getSeqNum() {
		return sequenceNum;
	}

	public int getTotalFields() {
		return totalFields;
	}

	public boolean isHash() {
		return this.field.isHash();
	}

	/**
	 * Returns a key value as string from the given data graph
	 * 
	 * @param dataGraph
	 *            the data graph
	 * @return the key value
	 */
	public abstract String getKey(commonj.sdo.DataGraph dataGraph);

	/**
	 * Returns a key value as string from the given data object
	 * 
	 * @param dataObject
	 *            the root data object
	 * @return the key value
	 */
	public abstract String getKey(DataObject dataObject);

	/**
	 * Returns a key value as bytes from the given data graph
	 * 
	 * @param dataGraph
	 *            the data graph
	 * @return the key value
	 */
	public abstract byte[] getKeyBytes(commonj.sdo.DataGraph dataGraph);

	/**
	 * Returns a key value as bytes from the given data object
	 * 
	 * @param dataObject
	 *            the root data object
	 * @return the key value
	 */
	public abstract byte[] getKeyBytes(DataObject dataObject);

	/**
	 * Returns the maximum length allowed for this key field.
	 * 
	 * @return the maximum length allowed for this key field.
	 */
	public abstract int getMaxLength();

	public abstract DataFlavor getDataFlavor();

}
