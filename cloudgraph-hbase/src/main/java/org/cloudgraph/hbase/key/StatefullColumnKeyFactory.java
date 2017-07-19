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
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.state.RowState;
import org.cloudgraph.store.key.EdgeMetaField;
import org.cloudgraph.store.key.EntityMetaField;
import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.cloudgraph.store.key.MetaField;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;


/**
 * Generates an HBase column key based on the configured CloudGraph column key {@link org.cloudgraph.config.ColumnKeyModel
 * model} for a specific HTable {@link org.cloudgraph.config.Table configuration}. 
 * <p>
 * The initial creation and subsequent reconstitution for query retrieval
 * purposes of both row and column keys in CloudGraph&#8482; is efficient, 
 * as it leverages byte array level API in both Java and the current 
 * underlying SDO 2.1 implementation, <a target="#" href="http://plasma-sdo.org">PlasmaSDO&#8482;</a>. Both composite row and 
 * column keys are composed in part of structural metadata, and the 
 * lightweight metadata API within <a target="#" href="http://plasma-sdo.org">PlasmaSDO&#8482;</a> contains byte-array level, 
 * cached lookup of all basic metadata elements including logical and 
 * physical type and property names.  
 * </p>
 * @see org.cloudgraph.config.ColumnKeyModel
 * @see org.cloudgraph.config.Table
 * @see org.cloudgraph.state.SequenceGenerator
 * @author Scott Cinnamond
 * @since 0.5
 */
public class StatefullColumnKeyFactory extends CompositeColumnKeyFactory 
    implements GraphStatefullColumnKeyFactory 
{
	private static final Log log = LogFactory.getLog(StatefullColumnKeyFactory.class);
		

	public StatefullColumnKeyFactory(PlasmaType rootType) {
		super(rootType);
	}
	
	public StatefullColumnKeyFactory(RowState graphRow) {
		super(graphRow);
	}

	@Override
	public byte[] createColumnKey( 
			PlasmaDataObject dataObject, long sequenceNum, PlasmaProperty property)	
	{
 	    PlasmaType type = (PlasmaType)dataObject.getType();
		return getKey(type, sequenceNum, property);
	}
	
	@Override
	public byte[] createColumnKey(PlasmaDataObject dataObject, long sequenceNum,
			PlasmaProperty property, EdgeMetaField metaField) {
	    PlasmaType type = (PlasmaType)dataObject.getType();
		return getKey(type, sequenceNum, property, metaField);
	}

	@Override
	public byte[] createColumnKey(PlasmaType type, long sequenceNum,
			PlasmaProperty property, EdgeMetaField metaField) {
		return getKey(type, sequenceNum, property, metaField);
	}
	
	@Override
	public byte[] createColumnKey(PlasmaType type, long sequenceNum,
			PlasmaProperty property, EntityMetaField metaField) {
		return getKey(type, sequenceNum, property, metaField);
	}

	@Override
	public byte[] createColumnKey(PlasmaType type, long sequenceNum,
			EntityMetaField metaField) {
		return getKey(type, sequenceNum, metaField);
	}
	
	@Override
	public byte[] createColumnKey(PlasmaType type, 
			long dataObjectSeqNum, PlasmaProperty property)	
	{
		return getKey(type, dataObjectSeqNum, property);
	}
	
	@Override
	public byte[] createColumnKey(PlasmaType type, long sequenceNum,
			EdgeMetaField metaField) {
		return getKey(type, sequenceNum, metaField);
	}
		
	private byte[] getKey(PlasmaType type, 
			long sequenceNum, PlasmaProperty property) 
	{
		if (sequenceNum <= 0)
			throw new IllegalArgumentException("expected positive sequence number not " + sequenceNum);
		byte[] seqNumBytes = Bytes.toBytes(String.valueOf(sequenceNum));
		byte[] sectionDelim = this.getGraph().getColumnKeySequenceDelimiterBytes();	    	    
		byte[] prefix = super.createColumnKey(type, property);
		byte[] result = concat(prefix, sectionDelim, seqNumBytes);
		if (log.isDebugEnabled())
			log.debug("key: " + Bytes.toString(result));
		
		return result;		
	}
	
	private byte[] getKey(PlasmaType type, 
			long sequenceNum, PlasmaProperty property,
			MetaField metaField) 
	{
		if (sequenceNum <= 0)
			throw new IllegalArgumentException("expected positive sequence number not " + sequenceNum);
		byte[] seqNumBytes = Bytes.toBytes(String.valueOf(sequenceNum));
		byte[] sectionDelim = this.getGraph().getColumnKeySequenceDelimiterBytes();	    	    
		byte[] prefix = super.createColumnKey(type, property);
		byte[] metaDelim = this.getGraph().getColumnKeyReferenceMetadataDelimiterBytes();		
		byte[] result = concat(prefix, sectionDelim, seqNumBytes, metaDelim, metaField.asBytes());
		if (log.isDebugEnabled())
			log.debug("key: " + Bytes.toString(result));
		
		return result;		
	}
	
	private byte[] getKey(PlasmaType type, 
			long sequenceNum, EntityMetaField metaField) 
	{
		if (sequenceNum <= 0)
			throw new IllegalArgumentException("expected positive sequence number not " + sequenceNum);
		byte[] seqNumBytes = Bytes.toBytes(String.valueOf(sequenceNum));
		byte[] sectionDelim = this.getGraph().getColumnKeySequenceDelimiterBytes();	

		byte[] prefix = super.createColumnKey(type, metaField);
				
		byte[] result = concat(prefix, sectionDelim, seqNumBytes);
		if (log.isDebugEnabled())
			log.debug("key: " + Bytes.toString(result));
		
		return result;		
	}

	private byte[] getKey(PlasmaType type, 
			long sequenceNum, EdgeMetaField metaField) 
	{
		if (sequenceNum <= 0)
			throw new IllegalArgumentException("expected positive sequence number not " + sequenceNum);
		byte[] seqNumBytes = Bytes.toBytes(String.valueOf(sequenceNum));
		byte[] sectionDelim = this.getGraph().getColumnKeySequenceDelimiterBytes();	

		byte[] prefix = super.createColumnKey(type, metaField);
				
		byte[] result = concat(prefix, sectionDelim, seqNumBytes);
		if (log.isDebugEnabled())
			log.debug("key: " + Bytes.toString(result));
		
		return result;		
	}
	
	private byte[] concat(byte[]... fields) {
		int i = 0;
		for (byte[] field : fields)
			i += field.length;
		byte[] result = new byte[i];
		i = 0;
		for (byte[] field : fields) {
			System.arraycopy(field, 0, result, i, field.length);
			i += field.length;
		}
		return result;
	}


}
