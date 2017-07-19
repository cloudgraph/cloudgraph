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
import java.util.Arrays;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.Pair;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.KeyFieldConfig;
import org.cloudgraph.config.PreDefinedKeyFieldConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.filter.HBaseFilterAssembler;
import org.cloudgraph.hbase.key.Hashing;
import org.cloudgraph.hbase.key.KeySupport;
import org.cloudgraph.hbase.key.Padding;
import org.plasma.query.Wildcard;
import org.plasma.query.model.Where;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaType;


/**
 * Assembles a composite fuzzy row key bytes / fuzzy info bytes pair 
 * where each field within the composite row keys are constructed 
 * based a set of query predicates.  
 * 
 * @see org.cloudgraph.config.DataGraphConfig
 * @see org.cloudgraph.config.TableConfig
 * @see org.cloudgraph.config.UserDefinedField 
 * @see org.cloudgraph.config.PredefinedField 
 * @see org.cloudgraph.config.PreDefinedFieldName 
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class FuzzyRowKeyScanAssembler  
    implements RowKeyScanAssembler, FuzzyRowKey, HBaseFilterAssembler
{
    private static Log log = LogFactory.getLog(FuzzyRowKeyScanAssembler.class);
	protected int bufsize = 4000;
	protected ByteBuffer keyBytes = ByteBuffer.allocate(bufsize);
	protected ByteBuffer infoBytes = ByteBuffer.allocate(bufsize);
	protected PlasmaType rootType;
	protected DataGraphConfig graph;
	protected TableConfig table;
	protected KeySupport keySupport = new KeySupport();
	protected Charset charset;
	protected ScanLiterals scanLiterals;
	protected int fieldCount;
    protected String rootUUID;
    protected byte fixedMaskByte = 0;
    protected byte variableMaskByte = 1;
    protected byte[] delimMask;
	protected Hashing hashing;
	protected Padding padding;
	
	@SuppressWarnings("unused")
	private FuzzyRowKeyScanAssembler() {}

	/**
	 * Constructor
	 * @param rootType the root type
	 */
	public FuzzyRowKeyScanAssembler(PlasmaType rootType)
	{
    	this.rootType = rootType;
		QName rootTypeQname = this.rootType.getQualifiedName();
		this.graph = CloudGraphConfig.getInstance().getDataGraph(
				rootTypeQname);
		this.table = CloudGraphConfig.getInstance().getTable(rootTypeQname);
		this.charset = CloudGraphConfig.getInstance().getCharset();
		this.delimMask = new byte[graph.getRowKeyFieldDelimiterBytes().length];
	    for (int i = 0; i < delimMask.length; i++)
	    	delimMask[i] = fixedMaskByte;
		Hash hash = this.keySupport.getHashAlgorithm(this.table);
		this.hashing = new Hashing(hash, this.charset);
		this.padding = new Padding(this.charset);

	}
	
	/**
	 * Constructor which enables the use of data object UUID as
	 * a pre-defined row key field. 
	 * @param rootType the root type
	 * @param rootUUID the root UUID.
	 */
	public FuzzyRowKeyScanAssembler(PlasmaType rootType, String rootUUID)
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
    	List<PreDefinedKeyFieldConfig> preDefinedFields = this.graph.getPreDefinedRowKeyFields();
        int fieldCount = preDefinedFields.size();
    	for (int i = 0; i < fieldCount; i++) {
        	PreDefinedKeyFieldConfig preDefinedField = preDefinedFields.get(i);
    		if (fieldCount > 0) {
        	    this.keyBytes.put(graph.getRowKeyFieldDelimiterBytes());
    			this.infoBytes.put(delimMask);
    		}
    		byte[] tokenValue = this.keySupport.getPredefinedFieldValueStartBytes(this.rootType, 
       	    		hashing, preDefinedField);
    		
       	    byte[] paddedTokenValue = null;
       	    if (preDefinedField.isHash()) {
       	    	paddedTokenValue = this.padding.pad(tokenValue, preDefinedField.getMaxLength(), 
    				DataFlavor.integral);
       	    }
       	    else {
       	    	paddedTokenValue = this.padding.pad(tokenValue, preDefinedField.getMaxLength(), 
       	    			preDefinedField.getDataFlavor());
       	    }
    		
    		this.keyBytes.put(paddedTokenValue);
			byte[] tokenMask = new byte[tokenValue.length];
    	    for (int j = 0; j < tokenMask.length; j++)
    	    	tokenMask[j] = fixedMaskByte;
			this.infoBytes.put(tokenMask);
    		
    		fieldCount++;
        }				
	}
	
	private void assembleLiterals()
	{		
	    for (KeyFieldConfig fieldConfig : this.graph.getRowKeyFields()) {
    		if (fieldCount > 0) {
        	    this.keyBytes.put(graph.getRowKeyFieldDelimiterBytes());
    			this.infoBytes.put(delimMask);
    		}
			
    		if (fieldConfig instanceof PreDefinedKeyFieldConfig) {
    			PreDefinedKeyFieldConfig predefinedConfig = (PreDefinedKeyFieldConfig)fieldConfig;        		
        		byte[] tokenValue = getPredefinedToken(predefinedConfig);
        		
           	    byte[] paddedTokenValue = null;
           	    if (predefinedConfig.isHash()) {
           	    	paddedTokenValue = this.padding.pad(tokenValue, 
           	    		predefinedConfig.getMaxLength(), 
        				DataFlavor.integral);
           	    }
           	    else {
           	    	paddedTokenValue = this.padding.pad(tokenValue, 
           	    		predefinedConfig.getMaxLength(), 
           	    		predefinedConfig.getDataFlavor());
           	    }
           	    this.keyBytes.put(paddedTokenValue);
    			byte[] tokenMask = getPredefinedTokenMask(predefinedConfig, paddedTokenValue.length);
    			this.infoBytes.put(tokenMask);
           	    
           	    this.fieldCount++;
    		}
    		else if (fieldConfig instanceof UserDefinedRowKeyFieldConfig) {
    			UserDefinedRowKeyFieldConfig userFieldConfig = (UserDefinedRowKeyFieldConfig)fieldConfig;

    			List<ScanLiteral> scanLiterals = this.scanLiterals.getLiterals(userFieldConfig);    				 
    			// if no literal present, create a fuzzy wildcard
    			// as per the field length
    			if (scanLiterals == null || scanLiterals.size() == 0) {
    				byte[] wildcard = new byte[userFieldConfig.getMaxLength()];
    				Arrays.fill(wildcard, (byte)Character.valueOf('*').charValue());   				
    				this.keyBytes.put(wildcard); // no need for padding, we are at max len
        			byte[] fieldMask = new byte[userFieldConfig.getMaxLength()];
            	    for (int i = 0; i < fieldMask.length; i++)
            	    	fieldMask[i] = this.variableMaskByte;
        			this.infoBytes.put(fieldMask);
    			}
    			else {	 
    				if (scanLiterals.size() > 1)
    					log.warn("expected single literal for user defined field - ignoring");
    				FuzzyRowKeyLiteral fuzzyLiteral = (FuzzyRowKeyLiteral)scanLiterals.get(0);
    				if (WildcardStringLiteral.class.isAssignableFrom(fuzzyLiteral.getClass())) {
    					WildcardStringLiteral wc = (WildcardStringLiteral)fuzzyLiteral;
    					String literal = wc.getContent();
    					if (literal.length() != wc.getFieldConfig().getMaxLength()) {
    						if (literal.startsWith(Wildcard.WILDCARD_CHAR) && literal.endsWith(Wildcard.WILDCARD_CHAR)) {
    							throw new InvalidFuzzyRowKeyWildcardException(
    								"wildcard expression length does not equal field max length ("+wc.getFieldConfig().getMaxLength()+") for field '"
    							    +wc.getFieldConfig().getEndpointProperty().getName()+"' - fuzzy row key scan wildcard expressions, starting"
    								+ " and ending with a wildcard character, must exactly match (typically by padding with wildcards) the length of the target (composite)"
    								+ " row key field, as HBase fuzzy row key filters use fixed length masking.");
    						}
    					}
    				}
    				// note; already padded
    				this.keyBytes.put(fuzzyLiteral.getFuzzyKeyBytes());
    				this.infoBytes.put(fuzzyLiteral.getFuzzyInfoBytes());
    			}
    			
           	    this.fieldCount++;
    		}
		}			
	}
	
	private byte[] getPredefinedToken(PreDefinedKeyFieldConfig predefinedConfig) {
		byte[] tokenValue = null;
		switch (predefinedConfig.getName()) {
		case UUID:
			if (this.rootUUID != null) {
				tokenValue = this.rootUUID.getBytes(this.charset);
			}
			else {
				// no UUID available in this context, so return zero len array
				// which will get padded with default wildcard bytes and associated
				// with a variable info-mask bytes array
				byte wc = (byte)Wildcard.WILDCARD_CHAR.charAt(0);
				tokenValue = new byte[predefinedConfig.getMaxLength()];
				for (int i = 0; i < tokenValue.length; i++)
					tokenValue[i] = wc;
			}
			break;
		default:	
		    tokenValue = predefinedConfig.getKeyBytes(this.rootType);
			break;
		}        		
		
		if (predefinedConfig.isHash()) {
			tokenValue = hashing.toStringBytes(tokenValue);
		}    			
		return tokenValue;
	}

	private byte[] getPredefinedTokenMask(PreDefinedKeyFieldConfig predefinedConfig, int paddedTokenValueLength) {
		byte[] tokenMask = new byte[paddedTokenValueLength];
		switch (predefinedConfig.getName()) {
		case UUID:
			if (this.rootUUID != null) {
				Arrays.fill(tokenMask, fixedMaskByte); 
			}
			else {
				Arrays.fill(tokenMask, variableMaskByte); 
			}
			break;
		default:	
			Arrays.fill(tokenMask, fixedMaskByte); 
			break;
		}        		
		return tokenMask;
	}
	
	@Override
	public byte[] getFuzzyKeyBytes() {
		if (this.keyBytes == null)
			throw new IllegalStateException("row keys not assembled - first call assemble(...)");
	    byte [] result = new byte[this.keyBytes.position()];
	    System.arraycopy(this.keyBytes.array(), this.keyBytes.arrayOffset(), result, 0, this.keyBytes.position()); 
		return result;
	}

	@Override
	public byte[] getFuzzyInfoBytes() {
		if (this.infoBytes == null)
			throw new IllegalStateException("row keys not assembled - first call assemble(...)");
	    byte [] result = new byte[this.infoBytes.position()];
	    System.arraycopy(this.infoBytes.array(), this.infoBytes.arrayOffset(), result, 0, this.infoBytes.position()); 
		return result;
	}

	@Override
	public void clear() {
		this.scanLiterals = null;	
	}

	@Override
	public Filter getFilter() {
		Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>(this.getFuzzyKeyBytes(), getFuzzyInfoBytes());
		List<Pair<byte[], byte[]>> list = new ArrayList<Pair<byte[], byte[]>>();
		list.add(pair);
		return new FuzzyRowFilter(list);
	}
}
