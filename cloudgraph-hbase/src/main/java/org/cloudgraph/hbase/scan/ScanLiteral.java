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
package org.cloudgraph.hbase.scan;

import java.nio.charset.Charset;

import javax.xml.namespace.QName;

import org.apache.hadoop.hbase.util.Hash;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.key.Hashing;
import org.cloudgraph.hbase.key.KeySupport;
import org.cloudgraph.hbase.key.Padding;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.model.RelationalOperator;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.helper.DataConverter;

/**
 * A configuration driven, context rich abstract class where subclasses are data
 * "flavor" and data type specific and used to abstract the complexities
 * involved in assembling the various segments and fields of composite (scan
 * start/stop) row keys under various relational and logical operator and
 * various configurable composite key-field hashing, formatting, padding and
 * other features.
 * 
 * @see org.cloudgraph.config.TableConfig
 * @author Scott Cinnamond
 * @since 0.5
 */
public abstract class ScanLiteral {
	protected final int HASH_INCREMENT = 1;

	protected String literal;
	@Deprecated
	protected RelationalOperator relationalOperator;
	protected UserDefinedRowKeyFieldConfig fieldConfig;
	protected DataConverter dataConverter = DataConverter.INSTANCE;
	protected PlasmaType rootType;
	protected Charset charset;
	protected TableConfig table;
	protected KeySupport keySupport = new KeySupport();
	protected PlasmaProperty property;
	protected Hashing hashing;
	protected Padding padding;

	@SuppressWarnings("unused")
	private ScanLiteral() {
	}

	public ScanLiteral(String literal, PlasmaType rootType,
			RelationalOperator relationalOperator,
			UserDefinedRowKeyFieldConfig fieldConfig) {
		super();
		this.rootType = rootType;
		this.relationalOperator = relationalOperator;
		this.fieldConfig = fieldConfig;
		this.property = (PlasmaProperty) this.fieldConfig.getEndpointProperty();
		this.literal = literal;

		QName rootTypeQname = this.rootType.getQualifiedName();
		this.table = CloudGraphConfig.getInstance().getTable(rootTypeQname);
		Hash hash = this.keySupport.getHashAlgorithm(this.table);
		this.charset = CloudGraphConfig.getInstance().getCharset();
		this.hashing = new Hashing(hash, this.charset);
		this.padding = new Padding(this.charset);
	}

	/**
	 * Returns the string literal
	 * 
	 * @return the string literal
	 */
	public final String getContent() {
		return literal;
	}

	/**
	 * Returns the context relational operator.
	 * 
	 * @return the context relational operator.
	 */
	public final RelationalOperator getRelationalOperator() {
		return relationalOperator;
	}

	/**
	 * Returns the composite row-key field configuration.
	 * 
	 * @return the composite row-key field configuration.
	 */
	public final UserDefinedRowKeyFieldConfig getFieldConfig() {
		return fieldConfig;
	}

	/**
	 * Returns the HBase scan "start-row" composite row-key field bytes for this
	 * literal under the various relational, logical operator and other
	 * optionally configurable hashing, formatting and padding features.
	 * 
	 * @return the HBase scan "start-row" composite row-key field bytes for this
	 *         literal under the various relational, logical operator and other
	 *         optionally configurable hashing, formatting and padding features.
	 */
	public byte[] getStartBytes() {
		switch (this.relationalOperator.getValue()) {
			case EQUALS :
				return getEqualsStartBytes();
			case GREATER_THAN :
				return getGreaterThanStartBytes();
			case GREATER_THAN_EQUALS :
				return getGreaterThanEqualStartBytes();
			case LESS_THAN :
				return getLessThanStartBytes();
			case LESS_THAN_EQUALS :
				return getLessThanEqualStartBytes();
			case NOT_EQUALS :
			default :
				throw new GraphServiceException("relational operator '"
						+ this.relationalOperator.getValue().toString()
						+ "' not supported for integral row key fields");
		}
	}

	/**
	 * Returns the HBase scan "stop-row" composite row-key field bytes for this
	 * literal under the various relational, logical operator and other
	 * optionally configurable hashing, formatting and padding features.
	 * 
	 * @return the HBase scan "stop-row" composite row-key field bytes for this
	 *         literal under the various relational, logical operator and other
	 *         optionally configurable hashing, formatting and padding features.
	 */
	public byte[] getStopBytes() {
		switch (this.relationalOperator.getValue()) {
			case EQUALS :
				return getEqualsStopBytes();
			case GREATER_THAN :
				return getGreaterThanStopBytes();
			case GREATER_THAN_EQUALS :
				return getGreaterThanEqualStopBytes();
			case LESS_THAN :
				return getLessThanStopBytes();
			case LESS_THAN_EQUALS :
				return getLessThanEqualStopBytes();
			case NOT_EQUALS :
			default :
				throw new GraphServiceException("relational operator '"
						+ this.relationalOperator.getValue().toString()
						+ "' not supported for integral row key fields");
		}
	}

	/**
	 * Returns the "start row" bytes used to represent "equals" relational
	 * operator under an HBase partial row-key scan for this literal under the
	 * various optionally configurable hashing, formatting and padding features.
	 * 
	 * @return the "start row" bytes used to represent "equals" relational
	 *         operator under an HBase partial row-key scan for this literal
	 *         under the various optionally configurable hashing, formatting and
	 *         padding features.
	 */
	public abstract byte[] getEqualsStartBytes();

	/**
	 * Returns the "stop row" bytes used to represent "equals" relational
	 * operator under an HBase partial row-key scan for this literal under the
	 * various optionally configurable hashing, formatting and padding features.
	 * 
	 * @return the "stop row" bytes used to represent "equals" relational
	 *         operator under an HBase partial row-key scan for this literal
	 *         under the various optionally configurable hashing, formatting and
	 *         padding features.
	 */
	public abstract byte[] getEqualsStopBytes();

	/**
	 * Returns the "start row" bytes used to represent "greater than" relational
	 * operator under an HBase partial row-key scan for this literal under the
	 * various optionally configurable hashing, formatting and padding features.
	 * 
	 * @return the "start row" bytes used to represent "greater than" relational
	 *         operator under an HBase partial row-key scan for this literal
	 *         under the various optionally configurable hashing, formatting and
	 *         padding features.
	 */
	public abstract byte[] getGreaterThanStartBytes();

	/**
	 * Returns the "stop row" bytes used to represent "greater than" relational
	 * operator under an HBase partial row-key scan for this literal under the
	 * various optionally configurable hashing, formatting and padding features.
	 * 
	 * @return the "stop row" bytes used to represent "greater than" relational
	 *         operator under an HBase partial row-key scan for this literal
	 *         under the various optionally configurable hashing, formatting and
	 *         padding features.
	 */
	public abstract byte[] getGreaterThanStopBytes();

	/**
	 * Returns the "start row" bytes used to represent "greater than equals"
	 * relational operator under an HBase partial row-key scan for this literal
	 * under the various optionally configurable hashing, formatting and padding
	 * features.
	 * 
	 * @return the "start row" bytes used to represent "greater than equals"
	 *         relational operator under an HBase partial row-key scan for this
	 *         literal under the various optionally configurable hashing,
	 *         formatting and padding features.
	 */
	public abstract byte[] getGreaterThanEqualStartBytes();

	/**
	 * Returns the "stop row" bytes used to represent "greater than equals"
	 * relational operator under an HBase partial row-key scan for this literal
	 * under the various optionally configurable hashing, formatting and padding
	 * features.
	 * 
	 * @return the "stop row" bytes used to represent "greater than equals"
	 *         relational operator under an HBase partial row-key scan for this
	 *         literal under the various optionally configurable hashing,
	 *         formatting and padding features.
	 */
	public abstract byte[] getGreaterThanEqualStopBytes();

	/**
	 * Returns the "start row" bytes used to represent "less than" relational
	 * operator under an HBase partial row-key scan for this literal under the
	 * various optionally configurable hashing, formatting and padding features.
	 * 
	 * @return the "start row" bytes used to represent "less than" relational
	 *         operator under an HBase partial row-key scan for this literal
	 *         under the various optionally configurable hashing, formatting and
	 *         padding features.
	 */
	public abstract byte[] getLessThanStartBytes();

	/**
	 * Returns the "stop row" bytes used to represent "less than" relational
	 * operator under an HBase partial row-key scan for this literal under the
	 * various optionally configurable hashing, formatting and padding features.
	 * 
	 * @return the "stop row" bytes used to represent "less than" relational
	 *         operator under an HBase partial row-key scan for this literal
	 *         under the various optionally configurable hashing, formatting and
	 *         padding features.
	 */
	public abstract byte[] getLessThanStopBytes();

	/**
	 * Returns the "start row" bytes used to represent "less than equals"
	 * relational operator under an HBase partial row-key scan for this literal
	 * under the various optionally configurable hashing, formatting and padding
	 * features.
	 * 
	 * @return the "start row" bytes used to represent "less than equals"
	 *         relational operator under an HBase partial row-key scan for this
	 *         literal under the various optionally configurable hashing,
	 *         formatting and padding features.
	 */
	public abstract byte[] getLessThanEqualStartBytes();

	/**
	 * Returns the "stop row" bytes used to represent "less than equals"
	 * relational operator under an HBase partial row-key scan for this literal
	 * under the various optionally configurable hashing, formatting and padding
	 * features.
	 * 
	 * @return the "stop row" bytes used to represent "less than equals"
	 *         relational operator under an HBase partial row-key scan for this
	 *         literal under the various optionally configurable hashing,
	 *         formatting and padding features.
	 */
	public abstract byte[] getLessThanEqualStopBytes();
}
