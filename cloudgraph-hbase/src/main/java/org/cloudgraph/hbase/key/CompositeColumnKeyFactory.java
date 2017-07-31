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
package org.cloudgraph.hbase.key;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.config.ColumnKeyFieldConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.KeyFieldConfig;
import org.cloudgraph.config.PreDefinedFieldName;
import org.cloudgraph.config.PreDefinedKeyFieldConfig;
import org.cloudgraph.state.RowState;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Creates an HBase column key based on the configured CloudGraph column key
 * {@link org.cloudgraph.config.ColumnKeyModel model} for a specific HTable
 * {@link org.cloudgraph.config.Table configuration}.
 * <p>
 * The initial creation and subsequent reconstitution for query retrieval
 * purposes of both row and column keys in CloudGraph&#8482; is efficient, as it
 * leverages byte array level API in both Java and the current underlying SDO
 * 2.1 implementation, <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a>. Both composite row and
 * column keys are composed in part of structural metadata, and the lightweight
 * metadata API within <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a> contains byte-array level,
 * cached lookup of all basic metadata elements including logical and physical
 * type and property names.
 * </p>
 * 
 * @see org.cloudgraph.config.ColumnKeyModel
 * @see org.cloudgraph.config.Table
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CompositeColumnKeyFactory extends ByteBufferKeyFactory
		implements
			GraphColumnKeyFactory {
	private static final Log log = LogFactory
			.getLog(CompositeColumnKeyFactory.class);

	public CompositeColumnKeyFactory(PlasmaType rootType) {
		super(rootType);
	}
	public CompositeColumnKeyFactory(RowState graphRow) {
		super(graphRow);
		// TODO Auto-generated constructor stub
	}

	@Override
	public byte[] createColumnKey(PlasmaType type, PlasmaProperty property) {

		this.buf.clear();
		addColumnKeyFields(type, property);

		// ByteBuffer.array() returns unsized array so don't sent that back to
		// clients
		// to misuse.
		// Use native arraycopy() method as it uses native memcopy to create
		// result array
		// and because and
		// ByteBuffer.get(byte[] dst,int offset, int length) is not native
		byte[] result = new byte[this.buf.position()];
		System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0,
				this.buf.position());

		return result;
	}

	@Override
	public byte[] createColumnKey(PlasmaType type, EntityMetaKey metaField) {
		this.buf.clear();
		addColumnKeyFields(type, metaField);
		byte[] result = new byte[this.buf.position()];
		System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0,
				this.buf.position());
		return result;
	}

	@Override
	public byte[] createColumnKey(PlasmaType type, EdgeMetaKey metaField) {
		this.buf.clear();
		addColumnKeyFields(type, metaField);
		byte[] result = new byte[this.buf.position()];
		System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0,
				this.buf.position());
		return result;
	}

	@Override
	public byte[] createColumnKey(PlasmaType type, PlasmaProperty property,
			EntityMetaKey metaField) {
		this.buf.clear();
		addColumnKeyFields(type, property);
		this.buf.put(this.getGraph()
				.getColumnKeyReferenceMetadataDelimiterBytes());
		this.buf.put(metaField.codeAsBytes());
		byte[] result = new byte[this.buf.position()];
		System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0,
				this.buf.position());
		return result;
	}

	@Override
	public byte[] createColumnKey(PlasmaType type, PlasmaProperty property,
			EdgeMetaKey metaField) {
		this.buf.clear();
		addColumnKeyFields(type, property);
		this.buf.put(this.getGraph()
				.getColumnKeyReferenceMetadataDelimiterBytes());
		this.buf.put(metaField.codeAsBytes());
		byte[] result = new byte[this.buf.position()];
		System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0,
				this.buf.position());
		return result;
	}

	protected void addColumnKeyFields(PlasmaType type, EntityMetaKey metaField) {
		int i = 0;
		for (KeyFieldConfig fieldConfig : this.getGraph().getColumnKeyFields()) {
			if (i > 0)
				this.buf.put(this.getGraph().getColumnKeyFieldDelimiterBytes());
			PreDefinedKeyFieldConfig predefinedFieldConfig = (PreDefinedKeyFieldConfig) fieldConfig;
			byte[] keyValue = predefinedFieldConfig
					.getKeyBytes(type, metaField);
			if (fieldConfig.isHash()) {
				keyValue = this.hashing.toStringBytes(keyValue);
			}
			this.buf.put(keyValue);
			i++;
		}
	}

	protected void addColumnKeyFields(PlasmaType type, EdgeMetaKey metaField) {
		int i = 0;
		for (KeyFieldConfig fieldConfig : this.getGraph().getColumnKeyFields()) {
			if (i > 0)
				this.buf.put(this.getGraph().getColumnKeyFieldDelimiterBytes());
			PreDefinedKeyFieldConfig predefinedFieldConfig = (PreDefinedKeyFieldConfig) fieldConfig;
			byte[] keyValue = predefinedFieldConfig
					.getKeyBytes(type, metaField);
			if (fieldConfig.isHash()) {
				keyValue = this.hashing.toStringBytes(keyValue);
			}
			this.buf.put(keyValue);
			i++;
		}
	}

	protected void addColumnKeyFields(PlasmaType type, PlasmaProperty property) {
		int i = 0;
		for (KeyFieldConfig fieldConfig : this.getGraph().getColumnKeyFields()) {
			if (i > 0)
				this.buf.put(this.getGraph().getColumnKeyFieldDelimiterBytes());
			PreDefinedKeyFieldConfig predefinedFieldConfig = (PreDefinedKeyFieldConfig) fieldConfig;
			byte[] keyValue = predefinedFieldConfig.getKeyBytes(type, property);
			if (fieldConfig.isHash()) {
				keyValue = this.hashing.toStringBytes(keyValue);
			}
			this.buf.put(keyValue);
			i++;
		}
	}

	protected byte[] configureTokenBytes(byte[] token, DataGraphConfig graph,
			Hashing hashing, PreDefinedFieldName tokenName) {
		byte[] result = token;
		ColumnKeyFieldConfig tokenConfig = graph.getColumnKeyField(tokenName);
		if (tokenConfig != null) {
			if (tokenConfig.isHash()) {
				result = hashing.toStringBytes(result);
			}
		}
		return result;
	}

}
