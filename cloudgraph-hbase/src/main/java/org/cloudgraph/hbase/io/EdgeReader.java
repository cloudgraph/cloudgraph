package org.cloudgraph.hbase.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.store.key.EdgeMetaField;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

public class EdgeReader extends DefaultEdgeOperation {
	private static Log log = LogFactory.getLog(EdgeReader.class);
	private PlasmaType oppositeType;
	private boolean oppositeTypeBound;
	
    EdgeReader(PlasmaType sourceType,
			PlasmaProperty sourceProp, Long typeSequenceNum,
			TableConfig tableConfig, DataGraphConfig graphConfig, 
			RowReader rowReader) throws IOException {
		 
		super(sourceType, sourceProp, tableConfig, graphConfig);
		
		this.family = tableConfig.getDataColumnFamilyNameBytes();		
		this.baseTypeQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, typeSequenceNum, sourceProp, EdgeMetaField._BTP_);
		this.pathQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, typeSequenceNum, sourceProp, EdgeMetaField._PTH_);
		this.sequencesQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, typeSequenceNum, sourceProp, EdgeMetaField._SQS_);
		this.subTypeQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, typeSequenceNum, sourceProp, EdgeMetaField._STP_);
		//this.tableQual = rowReader.getColumnKeyFactory().createColumnKey(
		//		this.sourceType, typeSequenceNum, sourceProp, EdgeMetaField._TBL_);
		this.keysQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, typeSequenceNum, sourceProp, EdgeMetaField._KYS_);
		this.countQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, typeSequenceNum, sourceProp, EdgeMetaField._CNT_);
		
		this.typeQualStr = new String(this.baseTypeQual, this.charset);
		this.pathQualStr = new String(this.pathQual, this.charset);
		this.sequencesQualStr = new String(this.sequencesQual, this.charset);
		//this.tableQualStr = new String(this.tableQual, this.charset);
		this.keysQualStr = new String(this.keysQual, this.charset);
		this.countQualStr = new String(this.countQual, this.charset);
		
		this.construct(rowReader, graphConfig);		
	}

    EdgeReader(PlasmaType targetType,
			PlasmaProperty sourceProp,  
			TableConfig tableConfig, DataGraphConfig graphConfig,
			RowReader rowReader) throws IOException {
		super(targetType, sourceProp,  tableConfig, graphConfig);
		
		this.family = tableConfig.getDataColumnFamilyNameBytes();		
		this.baseTypeQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, sourceProp, EdgeMetaField._BTP_);
		this.pathQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, sourceProp, EdgeMetaField._PTH_);
		this.sequencesQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, sourceProp, EdgeMetaField._SQS_);
		this.subTypeQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, sourceProp, EdgeMetaField._STP_);
		//this.tableQual = rowReader.getColumnKeyFactory().createColumnKey(
		//		this.sourceType, sourceProp, EdgeMetaField._TBL_);
		this.keysQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, sourceProp, EdgeMetaField._KYS_);
		this.countQual = rowReader.getColumnKeyFactory().createColumnKey(
				this.sourceType, sourceProp, EdgeMetaField._CNT_);
		
		this.construct(rowReader, graphConfig);
	}
    
    static boolean exists(PlasmaType targetType,
			PlasmaProperty sourceProp, 
			TableConfig tableConfig, DataGraphConfig graphConfig, 
			RowReader rowReader) throws IOException {
    	byte[] fam = tableConfig.getDataColumnFamilyNameBytes();
    	byte[] typeQual = rowReader.getColumnKeyFactory().createColumnKey(
				targetType, sourceProp, EdgeMetaField._BTP_);   	
		return rowReader.getRow().containsColumn(fam, typeQual);
    }

    static boolean exists(PlasmaType targetType,
			PlasmaProperty sourceProp, Long typeSequenceNum,
			TableConfig tableConfig, DataGraphConfig graphConfig, 
			RowReader rowReader) throws IOException {
    	byte[] fam = tableConfig.getDataColumnFamilyNameBytes();
    	byte[] typeQual = rowReader.getColumnKeyFactory().createColumnKey(
				targetType, typeSequenceNum, sourceProp, EdgeMetaField._BTP_);   	
		return rowReader.getRow().containsColumn(fam, typeQual);
    }
    
 	private void construct(RowReader rowReader, DataGraphConfig graphConfig) {
		
		this.oppositeType = (PlasmaType)sourceProp.getType();
		this.oppositeTypeBound = CloudGraphConfig.getInstance().findTable(
				this.oppositeType.getQualifiedName()) != null;

		byte[] typeBytes = fetchValue(family, baseTypeQual, rowReader.getRow());
		decodeCollectionBaseType(typeBytes);
		
		byte[] pathBytes = findValue(family, pathQual, rowReader.getRow());
		if (pathBytes != null)
		    this.collectionPath = new String(pathBytes, this.charset);

		byte[] countBytes = fetchValue(family, countQual, rowReader.getRow());
		long count = Long.valueOf(new String(countBytes, charset));
		
	    byte[] sequenceBytes = findValue(family, sequencesQual, rowReader.getRow());
	    if (sequenceBytes != null) {
            this.decodeSequences(sequenceBytes);	
    		if (count != this.sequences.size())
    			throw new IllegalStateException("invalid value " + count + " for qualifier " 
    			        + EdgeMetaField._CNT_ + " ("+Bytes.toString(countQual)+") for key: '"
    					+ Bytes.toString(rowReader.getRowKey())+"' - expected " + this.sequences.size());
	    }
	    
		//byte[] tableBytes = findValue(family, tableQual, rowReader.getRow());
		//if (tableBytes != null)
	    //    this.table = new String(tableBytes, this.charset);			
		
		byte[] rowKeyBytes = findValue(family, keysQual, rowReader.getRow());
		if (rowKeyBytes != null) {
	        this.decodeRowKeys(rowKeyBytes);
    		if (count != this.rowKeys.size())
    			throw new IllegalStateException("invalid value " + count + " for qualifier " 
    			        + EdgeMetaField._CNT_ + " ("+Bytes.toString(countQual)+") for key: '"
    					+ Bytes.toString(rowReader.getRowKey())+"' - expected " + this.rowKeys.size());
		}
		
	    byte[] subtypesBytes = findValue(family, subTypeQual, rowReader.getRow());
	    if (subtypesBytes != null) {
            this.decodeDefaultSubType(subtypesBytes);	
	    }
		

	}

}
