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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.config.KeyFieldConfig;
import org.cloudgraph.config.PreDefinedKeyFieldConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.state.RowState;
import org.cloudgraph.store.key.GraphKeyException;
import org.cloudgraph.store.key.GraphRowKeyExpressionFactory;
import org.cloudgraph.store.key.KeyValue;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Type;

/**
 * Generates an HBase row key based on the configured CloudGraph
 * {@link org.cloudgraph.config.RowKeyModel Row Key Model} for a specific
 * {@link org.cloudgraph.config.Table HTable Configuration}.
 * <p>
 * The initial creation and subsequent re-constitution for query retrieval
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
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CompositeRowKeyExpressionFactory extends ByteBufferKeyFactory
		implements
			GraphRowKeyExpressionFactory {
	private static final Log log = LogFactory
			.getLog(CompositeRowKeyExpressionFactory.class);

	public CompositeRowKeyExpressionFactory(RowState graphRow) {
		super(graphRow);
	}

	public CompositeRowKeyExpressionFactory(PlasmaType rootType) {
		super(rootType);
	}

	@Override
	public String createRowKeyExpr(List<KeyValue> values) {
		StringBuilder result = new StringBuilder();

		if (values == null || values.size() == 0)
			throw new IllegalArgumentException(
					"expected non-null, non-zero length list argument 'values'");

		String keyValue = null;
		int i = 0;
		for (KeyFieldConfig fieldConfig : this.getGraph().getRowKeyFields()) {
			if (i > 0)
				this.buf.put(this.getGraph().getRowKeyFieldDelimiterBytes());
			if (fieldConfig instanceof PreDefinedKeyFieldConfig) {
				PreDefinedKeyFieldConfig predefinedConfig = (PreDefinedKeyFieldConfig) fieldConfig;
				keyValue = new String(predefinedConfig.getKeyBytes(this
						.getRootType()), this.charset);
			} else if (fieldConfig instanceof UserDefinedRowKeyFieldConfig) {
				UserDefinedRowKeyFieldConfig userFieldConfig = (UserDefinedRowKeyFieldConfig) fieldConfig;
				KeyValue found = findTokenValue(
						userFieldConfig.getPropertyPath(), values);
				// user has a configuration for this path
				if (found != null) {
					keyValue = String.valueOf(found.getValue());
					if (userFieldConfig.isHash()) {
						if (found.isWildcard())
							throw new GraphKeyException(
									"cannot create wildcard expression for user"
											+ " defined row-key field with XPath expression '"
											+ userFieldConfig
													.getPathExpression()
											+ "'"
											+ " for table '"
											+ this.getTable().getName()
											+ "' - this field is defined as using an integral hash algorithm which prevents the use of wildcards");
					} else if (found.isWildcard()) {
						String expr = getDataFlavorRegex(found.getProp()
								.getDataFlavor());
						String replaceExpr = "\\" + found.getWildcard();
						keyValue = keyValue.replaceAll(replaceExpr, expr);
					}
				} else {
					if (userFieldConfig.isHash())
						throw new GraphKeyException(
								"cannot default datatype expression for user"
										+ " defined row-key field with XPath expression '"
										+ userFieldConfig.getPathExpression()
										+ "'"
										+ " for table '"
										+ this.getTable().getName()
										+ "' - this field is defined as using an integral hash algorithm which prevents the use of wildcards");
					PlasmaProperty prop = (PlasmaProperty) userFieldConfig
							.getEndpointProperty();
					keyValue = getDataFlavorRegex(prop.getDataFlavor());
				}
			}

			if (fieldConfig.isHash()) {
				keyValue = this.hashing.toString(keyValue);
			}

			result.append(keyValue);

			i++;
		}

		return result.toString();
	}

	@Override
	public byte[] createRowKeyExprBytes(List<KeyValue> values) {

		if (values == null || values.size() == 0)
			throw new IllegalArgumentException(
					"expected non-null, non-zero length list argument 'values'");

		this.buf.clear();

		byte[] keyValue = null;
		int i = 0;
		for (KeyFieldConfig fieldConfig : this.getGraph().getRowKeyFields()) {
			if (i > 0)
				this.buf.put(this.getGraph().getRowKeyFieldDelimiterBytes());
			if (fieldConfig instanceof PreDefinedKeyFieldConfig) {
				PreDefinedKeyFieldConfig predefinedConfig = (PreDefinedKeyFieldConfig) fieldConfig;
				keyValue = predefinedConfig.getKeyBytes(this.getRootType());
			} else if (fieldConfig instanceof UserDefinedRowKeyFieldConfig) {
				UserDefinedRowKeyFieldConfig userFieldConfig = (UserDefinedRowKeyFieldConfig) fieldConfig;
				KeyValue found = findTokenValue(
						userFieldConfig.getPropertyPath(), values);
				// user has a configuration for this path
				if (found != null) {
					String keyValueString = String.valueOf(found.getValue());
					if (userFieldConfig.isHash()) {
						if (found.isWildcard())
							throw new GraphKeyException(
									"cannot create wildcard expression for user"
											+ " defined row-key field with XPath expression '"
											+ userFieldConfig
													.getPathExpression()
											+ "'"
											+ " for table '"
											+ this.getTable().getName()
											+ "' - this field is defined as using an integral hash algorithm which prevents the use of wildcards");
					} else if (found.isWildcard()) {
						String expr = getDataFlavorRegex(found.getProp()
								.getDataFlavor());
						String replaceExpr = "\\" + found.getWildcard();
						keyValueString = keyValueString.replaceAll(replaceExpr,
								expr);
					}
					keyValue = keyValueString.getBytes(charset);
				} else {
					if (userFieldConfig.isHash())
						throw new GraphKeyException(
								"cannot default datatype expression for user"
										+ " defined row-key field with XPath expression '"
										+ userFieldConfig.getPathExpression()
										+ "'"
										+ " for table '"
										+ this.getTable().getName()
										+ "' - this field is defined as using an integral hash algorithm which prevents the use of wildcards");
					PlasmaProperty prop = (PlasmaProperty) userFieldConfig
							.getEndpointProperty();
					keyValue = getDataFlavorRegex(prop.getDataFlavor())
							.getBytes(charset);
				}
			}

			if (fieldConfig.isHash()) {
				keyValue = this.hashing.toStringBytes(keyValue);
			}

			buf.put(keyValue);

			i++;
		}

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

	private KeyValue findTokenValue(String path, List<KeyValue> values) {
		for (KeyValue pair : values) {
			if (pair.getPropertyPath().equals(path))
				return pair;
		}
		return null;
	}

	private String getDataFlavorRegex(DataFlavor dataFlavor) {
		switch (dataFlavor) {
			case integral :
				return "[0-9\\-]+?";
			case real :
				return "[0-9\\-\\.]+?";
			default :
				return ".*?"; // any character zero or more times
		}
	}

	/**
	 * Returns true if the data graph configured for the given
	 * {@link commonj.sdo.Type type} has a user defined token which maps to the
	 * given property path.
	 * 
	 * @param type
	 *            the SDO type
	 * @param path
	 *            the property path
	 * @return true if the data graph configured for the given
	 *         {@link commonj.sdo.Type type} has a user defined token which maps
	 *         to the given property path.
	 */
	@Override
	public boolean hasUserDefinedRowKeyToken(Type type, String path) {
		return this.getGraph().getUserDefinedRowKeyField(path) != null;
	}

}
