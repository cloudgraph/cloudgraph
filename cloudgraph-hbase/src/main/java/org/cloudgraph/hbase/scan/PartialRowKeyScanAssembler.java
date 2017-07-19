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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Hash;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.KeyFieldConfig;
import org.cloudgraph.config.PreDefinedKeyFieldConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.key.Hashing;
import org.cloudgraph.hbase.key.KeySupport;
import org.cloudgraph.hbase.key.Padding;
import org.plasma.query.model.Where;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaType;


/**
 * Assembles a composite partial row (start/stop) key pair where each
 * field within the composite start and stop row keys are constructed 
 * based a set of query predicates.  
 * 
 * @see org.cloudgraph.config.DataGraphConfig
 * @see org.cloudgraph.config.TableConfig
 * @see org.cloudgraph.config.UserDefinedField 
 * @see org.cloudgraph.config.PredefinedField 
 * @see org.cloudgraph.config.PreDefinedFieldName 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class PartialRowKeyScanAssembler  
    implements RowKeyScanAssembler, PartialRowKey
{
    private static Log log = LogFactory.getLog(PartialRowKeyScanAssembler.class);
	protected int bufsize = 4000;
	protected ByteBuffer startKey = ByteBuffer.allocate(bufsize);
	protected ByteBuffer stopKey = ByteBuffer.allocate(bufsize);
	protected PlasmaType rootType;
	protected DataGraphConfig graph;
	protected TableConfig table;
	protected KeySupport keySupport = new KeySupport();
	protected Charset charset;
	protected ScanLiterals scanLiterals;
	protected int startRowFieldCount;
    protected int stopRowFieldCount;
    protected String rootUUID;
	protected Hashing hashing;
	protected Padding padding;
	
	@SuppressWarnings("unused")
	private PartialRowKeyScanAssembler() {}

	/**
	 * Constructor
	 * @param rootType the root type
	 */
	public PartialRowKeyScanAssembler(PlasmaType rootType)
	{
    	this.rootType = rootType;
		QName rootTypeQname = this.rootType.getQualifiedName();
		this.graph = CloudGraphConfig.getInstance().getDataGraph(
				rootTypeQname);
		this.table = CloudGraphConfig.getInstance().getTable(rootTypeQname);
		Hash hash = this.keySupport.getHashAlgorithm(this.table);
		this.charset = CloudGraphConfig.getInstance().getCharset();
		this.hashing = new Hashing(hash, this.charset);
		this.padding = new Padding(this.charset);
	}
	
	/**
	 * Constructor which enables the use of data object UUID as
	 * a pre-defined row key field. Only applicable for graph
	 * predicate "slice" queries. 
	 * @param rootType the root type
	 * @param rootUUID the root UUID.
	 */
	public PartialRowKeyScanAssembler(PlasmaType rootType, String rootUUID)
	{
		this(rootType);
		this.rootUUID = rootUUID;
	}
	
    /**
     * Assemble row key scan information based only on any
     * pre-defined row-key fields such as the
     * data graph root type or URI.
     * @see org.cloudgraph.config.PredefinedField 
     * @see org.cloudgraph.config.PreDefinedFieldName 
     */
	@Override
	public void assemble() {    	
		this.startKey = ByteBuffer.allocate(bufsize);
		this.stopKey = ByteBuffer.allocate(bufsize);
    	assemblePredefinedFields();
	}
	
	/**
	 * Assemble row key scan information based on the given
	 * scan literals as well as pre-defined row-key fields such as the
     * data graph root type or URI.
	 * @param literalList the scan literals
     * @see org.cloudgraph.config.PredefinedField 
     * @see org.cloudgraph.config.PreDefinedFieldName 
	 */
	@Override
	public void assemble(ScanLiterals literals) {
		this.scanLiterals = literals;
		this.startKey = ByteBuffer.allocate(bufsize);
		this.stopKey = ByteBuffer.allocate(bufsize);
    	assembleLiterals();
	}
	
    /**
     * Assemble row key scan information based on one or more
     * given query predicates.
     * @param where the row predicate hierarchy
     * @param contextType the context type which may be the root type or another
     * type linked by one or more relations to the root
     */
	@Override
	public void assemble(Where where, PlasmaType contextType) {
		this.startKey = ByteBuffer.allocate(bufsize);
		this.stopKey = ByteBuffer.allocate(bufsize);
		
		if (log.isDebugEnabled())
    		log.debug("begin traverse");
    	
		ScanLiteralAssembler literalAssembler = 
				new ScanLiteralAssembler(this.rootType);
    	where.accept(literalAssembler); // traverse
    	
    	this.scanLiterals = literalAssembler.getPartialKeyScanResult();
    	
    	if (log.isDebugEnabled())
    		log.debug("end traverse");      	

    	assembleLiterals();
    }
	
	private void assemblePredefinedFields()
	{
    	List<PreDefinedKeyFieldConfig> resultFields = new ArrayList<PreDefinedKeyFieldConfig>();
        
    	for (PreDefinedKeyFieldConfig field : this.graph.getPreDefinedRowKeyFields()) {
    		switch (field.getName()) {
    		case URI: 
    		case TYPE:
    			resultFields.add(field);
    			break;
    		case UUID:
    			break; // not applicable
    		default:
    		}
        }    	
    	
    	int fieldCount = resultFields.size();
    	for (int i = 0; i < fieldCount; i++) {
        	PreDefinedKeyFieldConfig preDefinedField = resultFields.get(i);
    		if (startRowFieldCount > 0) {
        	    this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
    		}
    		if (stopRowFieldCount > 0) {
        	    this.stopKey.put(graph.getRowKeyFieldDelimiterBytes());
    		}
    		
       	    byte[] paddedStartValue = getStartBytes(preDefinedField);
    		
    		this.startKey.put(paddedStartValue);
       	    startRowFieldCount++;
       	    
       	    byte[] paddedStopValue = getStopBytes(preDefinedField, i >= (fieldCount -1));
       	    this.stopKey.put(paddedStopValue);
       	    stopRowFieldCount++;
        }				
	}
	
	private void assembleLiterals()
	{
		// first collect the set of field configs which have literals or
		// predefined field config value(s), such that we can determine the
		// last field value.
    	List<KeyFieldConfig> resultFields = new ArrayList<KeyFieldConfig>();        
    	for (KeyFieldConfig fieldConfig : this.graph.getRowKeyFields()) {
    		if (fieldConfig instanceof PreDefinedKeyFieldConfig) {
    			PreDefinedKeyFieldConfig predefinedConfig = (PreDefinedKeyFieldConfig)fieldConfig;
       		    switch (predefinedConfig.getName()) {
        		case UUID:
        			if (this.rootUUID != null) 
            			resultFields.add(fieldConfig);
       				break;
        		default:	
        			resultFields.add(fieldConfig);
        			break;
        		}        		
    		}
    		else {
    			UserDefinedRowKeyFieldConfig userFieldConfig = (UserDefinedRowKeyFieldConfig)fieldConfig;
    			List<ScanLiteral> scanLiterals = this.scanLiterals.getLiterals(userFieldConfig);    				 
    			if (scanLiterals != null)
    				resultFields.add(fieldConfig);
    		}
        }    			
		
    	int fieldCount = resultFields.size();
    	for (int i = 0; i < fieldCount; i++) {
    		KeyFieldConfig fieldConfig = resultFields.get(i);
    		if (fieldConfig instanceof PreDefinedKeyFieldConfig) {
    			PreDefinedKeyFieldConfig predefinedConfig = (PreDefinedKeyFieldConfig)fieldConfig;
        		
        		byte[] paddedStartValue = getStartBytes(predefinedConfig);
        		byte[] paddedStopValue = getStopBytes(predefinedConfig, 
        				i >= (fieldCount -1));        		
        		
        		if (startRowFieldCount > 0) 
            	    this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
        		if (stopRowFieldCount > 0) 
            	    this.stopKey.put(graph.getRowKeyFieldDelimiterBytes());
    			
           	    this.startKey.put(paddedStartValue);
           	    this.stopKey.put(paddedStopValue);
           	    this.startRowFieldCount++;
           	    this.stopRowFieldCount++;
    		}
    		else if (fieldConfig instanceof UserDefinedRowKeyFieldConfig) {
    			UserDefinedRowKeyFieldConfig userFieldConfig = (UserDefinedRowKeyFieldConfig)fieldConfig;
    			List<ScanLiteral> scanLiterals = this.scanLiterals.getLiterals(userFieldConfig);    				 
    			// We may have multiple literals but all may not have start/stop bytes, e.g.
    			// a String literal with a less-than-equal '<=' operator will not have a start bytes.
    			for (ScanLiteral scanLiteral : scanLiterals) {
    				byte[] startBytes = scanLiteral.getStartBytes();
    				if (startBytes.length > 0) {
    					if (this.startRowFieldCount > 0) {
    						this.startKey.put(graph.getRowKeyFieldDelimiterBytes());		
    					}
    					this.startKey.put(startBytes);
    					this.startRowFieldCount++;
    				}
    				
    				byte[] stopBytes = null;
        			// if not last field
        			if (i < (fieldCount -1)) {
        				stopBytes = scanLiteral.getStartBytes();
               	    }
               	    else {
               	    	// only use stop bytes is last field in (compound) key
               	    	stopBytes = scanLiteral.getStopBytes();
               	    }
    				
    				if (stopBytes.length > 0) {
    					if (this.stopRowFieldCount > 0) {
    						this.stopKey.put(graph.getRowKeyFieldDelimiterBytes());		
    					}
    					this.stopKey.put(stopBytes);
    					this.stopRowFieldCount++;
    				} 
    			}
    		}
		}			
	}
	
	private byte[] getStartBytes(PreDefinedKeyFieldConfig preDefinedField)
	{
		byte[] startValue = null;
		switch (preDefinedField.getName()) {
		case UUID:
			if (this.rootUUID != null) {
				startValue = this.rootUUID.getBytes(this.charset);
			}
			break;
		default:	
			startValue = this.keySupport.getPredefinedFieldValueStartBytes(this.rootType, 
       	    		hashing, preDefinedField);
			break;
		}        		
   	    byte[] paddedStartValue = null;
   	    if (preDefinedField.isHash()) {
   	    	paddedStartValue = this.padding.pad(startValue, preDefinedField.getMaxLength(), 
				DataFlavor.integral);
   	    }
   	    else {
   	    	paddedStartValue = this.padding.pad(startValue, preDefinedField.getMaxLength(), 
   	    			preDefinedField.getDataFlavor());
   	    }
		return paddedStartValue;
	}

	private byte[] getStopBytes(PreDefinedKeyFieldConfig preDefinedField, boolean lastField)
	{
		byte[] stopValue = null;
		switch (preDefinedField.getName()) {
		case UUID:
			if (this.rootUUID != null) {
				stopValue = this.rootUUID.getBytes(this.charset);
			}
			break;
		default:	
			if (!lastField)
			    stopValue = this.keySupport.getPredefinedFieldValueStartBytes(this.rootType, 
       	    		hashing, preDefinedField);
			else
				stopValue = this.keySupport.getPredefinedFieldValueStopBytes(this.rootType, 
	       	    		hashing, preDefinedField);
			break;
		}        		
   	    byte[] paddedStopValue = null;
   	    if (preDefinedField.isHash()) {
   	    	paddedStopValue = this.padding.pad(stopValue, preDefinedField.getMaxLength(), 
				DataFlavor.integral);
   	    }
   	    else {
   	    	paddedStopValue = this.padding.pad(stopValue, preDefinedField.getMaxLength(), 
   	    			preDefinedField.getDataFlavor());
   	    }
		return paddedStopValue;
	}
	
	/**
	 * Returns the start row key as a byte array.
	 * @return the start row key
	 * @throws IllegalStateException if row keys are not yet assembled
	 */
	@Override
	public byte[] getStartKey() {
		if (this.startKey == null)
			throw new IllegalStateException("row keys not assembled - first call assemble(...)");
		// ByteBuffer.array() returns unsized array so don't sent that back to clients
		// to misuse. 
		// Use native arraycopy() method as it uses native memcopy to create result array
		// and because 
		// ByteBuffer.get(byte[] dst,int offset, int length) is not native
	    byte [] result = new byte[this.startKey.position()];
	    System.arraycopy(this.startKey.array(), this.startKey.arrayOffset(), result, 0, this.startKey.position()); 
		return result;
	}

	/**
	 * Returns the stop row key as a byte array.
	 * @return the stop row key
	 * @throws IllegalStateException if row keys are not yet assembled
	 */
	@Override
	public byte[] getStopKey() {
		if (this.stopKey == null)
			throw new IllegalStateException("row keys not assembled - first call assemble(...)");
	    byte [] result = new byte[this.stopKey.position()];
	    System.arraycopy(this.stopKey.array(), this.stopKey.arrayOffset(), result, 0, this.stopKey.position()); 
		return result;
	}
}
