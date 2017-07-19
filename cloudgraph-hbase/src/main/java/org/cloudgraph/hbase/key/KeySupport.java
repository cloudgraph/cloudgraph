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

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.config.CloudGraphConfigurationException;
import org.cloudgraph.config.PreDefinedKeyFieldConfig;
import org.cloudgraph.config.PredefinedField;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.scan.StringLiteral;
import org.cloudgraph.hbase.service.CloudGraphContext;
import org.cloudgraph.store.key.KeyValue;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;

/**
 * Delegate class supporting composite key generation.
 * @author Scott Cinnamond
 * @since 0.5
 */
public class KeySupport {
	private static final Log log = LogFactory.getLog(CompositeRowKeyFactory.class);

	/**
	 * Returns the specific configured hash algorithm 
	 * configured for an HTable, or if not configured
	 * returns the configured HBase hash algorithm as
	 * configured within HBase using the 'hbase.hash.type'
	 * property.
	 * @return the specific configured hash algorithm 
	 * configured for an HTable.
	 */
	public Hash getHashAlgorithm(TableConfig table) {
		Hash hash = null; 
		if (table.hasHashAlgorithm()) {
			String hashName = table.getTable().getHashAlgorithm().getName().value();
		    hash = Hash.getInstance(Hash.parseHashType(hashName));
		}
		else {
		    String algorithm = CloudGraphContext.instance().getConfig().get(
		    		CloudGraphConstants.PROPERTY_CONFIG_HASH_TYPE);
		    if (algorithm != null)
		        hash = Hash.getInstance(Hash.parseHashType(algorithm));			
		}
		return hash;
	}

    public KeyValue findKeyValue(UserDefinedRowKeyFieldConfig fieldConfig, List<KeyValue> pairs) {
    	
    	commonj.sdo.Property fieldProperty = fieldConfig.getEndpointProperty();
    	commonj.sdo.Type fieldPropertyType = fieldProperty.getContainingType();
    	
		for (KeyValue keyValue : pairs) {
			if (keyValue.getProp().getName().equals(fieldConfig.getEndpointProperty().getName())) {
				if (keyValue.getProp().getContainingType().getName().equals(fieldPropertyType.getName())) {
					if (keyValue.getProp().getContainingType().getURI().equals(fieldPropertyType.getURI())) {
					    if (fieldConfig.getPropertyPath() != null) {
					    	if (keyValue.getPropertyPath() != null && 
					    		keyValue.getPropertyPath().equals(fieldConfig.getPropertyPath())) {
					    		return keyValue;
					    	}
					    }
					}					
				}
			}
		}
		return null;
    }    

	/**
	 * Returns a token value from the given Type
	 * @param type the SDO Type 
	 * @param hash the hash algorithm to use in the event the
	 * row key token is to be hashed 
	 * @param token the pre-defined row key token configuration
	 * @return the token value
	 */
    public String getPredefinedFieldValue(
			PlasmaType type, Hashing hashing, 
			PreDefinedKeyFieldConfig token) {
		String result = null;
		switch (token.getName()) {
		case URI: 
			result = type.getURIPhysicalName();
			if (result == null || result.length() == 0) {
				if (log.isDebugEnabled())
				    log.debug("no URI physical name for type, "
				    		+ type 
				    		+ ", defined - using logical name");
				result = type.getURI();
			}
			break;
		case TYPE:
			result = type.getPhysicalName();
			if (result == null || result.length() == 0) {
				if (log.isDebugEnabled())
				    log.debug("no physical name for type, "
				    		+ type 
				    		+ ", defined - using logical name");
				result = type.getName();
			}
			break;
		default:
		    throw new CloudGraphConfigurationException("invalid row key token name, "
		    		+ token.getName().name() + " - cannot get this token from a SDO Type");
		}
		
		if (token.isHash()) {
			result = hashing.toString(result);
		}
		
		return result;
	}
    
    public byte[] getPredefinedFieldValueStartBytes(
			PlasmaType type, Hashing hashing, 
			PreDefinedKeyFieldConfig token) {
    	return getPredefinedFieldValueBytes(type, hashing, token);
    }
    
    public byte[] getPredefinedFieldValueStopBytes(
			PlasmaType type, Hashing hashing, 
			PreDefinedKeyFieldConfig token) {
		byte[] result = null;
		switch (token.getName()) {
		case URI: 
			if (token.isHash()) {
				byte[] uriBytes = type.getURIPhysicalNameBytes();
				if (uriBytes == null || uriBytes.length == 0) {
					if (log.isDebugEnabled())
					    log.debug("no URI physical name for type, " + type 
					    		+ ", defined - using logical name");
					uriBytes = type.getURIBytes();
				}
				result = hashing.toStringBytes(uriBytes, 1);
			}
			else {
				String uriName = type.getURIPhysicalName();
				if (uriName == null || uriName.length() == 0) {
					if (log.isDebugEnabled())
					    log.debug("no URI physical name for type, " + type 
					    		+ ", defined - using logical name");
					uriName = type.getURI();
				}
				uriName += StringLiteral.INCREMENT;
				result = Bytes.toBytes(uriName);
			}
			break;
		case TYPE:
			if (token.isHash()) {
				byte[] nameBytes = type.getPhysicalNameBytes();
				if (nameBytes == null || nameBytes.length == 0) {
					if (log.isDebugEnabled())
					    log.debug("no physical name for type, " + type 
					    		+ ", defined - using logical name");
					nameBytes = type.getNameBytes();
				}
				result = hashing.toStringBytes(nameBytes, 1);
			}
			else {
				String nameString = type.getPhysicalName();
				if (nameString == null) {
					if (log.isDebugEnabled())
					    log.debug("no physical name for type, " + type 
					    		+ ", defined - using logical name");
					nameString = type.getName();
				}
				nameString += StringLiteral.INCREMENT;
				result = Bytes.toBytes(nameString);
			}
			break;
		default:
		    throw new CloudGraphConfigurationException("invalid row key token name, "
		    		+ token.getName().name() + " - cannot get this token from a SDO Type");
		}
		
		return result;
    }

    public byte[] getPredefinedFieldValueBytes(
			PlasmaType type, Hashing hashing, 
			PreDefinedKeyFieldConfig token) {
		byte[] result = null;
		switch (token.getName()) {
		case URI: 
			result = type.getURIPhysicalNameBytes();
			if (result == null || result.length == 0) {
				if (log.isDebugEnabled())
				    log.debug("no URI physical name for type, " + type 
				    		+ ", defined - using logical name");
				result = type.getURIBytes();
			}
			break;
		case TYPE:
			result = type.getPhysicalNameBytes();
			if (result == null || result.length == 0) {
				if (log.isDebugEnabled())
				    log.debug("no physical name for type, " + type 
				    		+ ", defined - using logical name");
				result = type.getNameBytes();
			}
			break;
		default:
		    throw new CloudGraphConfigurationException("invalid row key token name, "
		    		+ token.getName().name() + " - cannot get this token from a SDO Type");
		}
		
		if (token.isHash()) {
			result = hashing.toStringBytes(result);
		}
		
		return result;
	}
	
	/**
	 * Returns a token value from the given Data Graph
	 * @param dataGraph the data graph 
	 * @param hash the hash algorithm to use in the event the
	 * row key token is to be hashed 
	 * @param token the pre-defined row key token configuration
	 * @return the token value
	 */
	public byte[] getPredefinedFieldValueBytes(
			DataGraph dataGraph, Hash hash, 
			PredefinedField token) {
		return getPredefinedFieldValueBytes(dataGraph.getRootObject(),
			hash, token);
	}
    
    /**
	 * Returns a token value from the given data object
	 * @param dataObject the root data object 
	 * @param hash the hash algorithm to use in the event the
	 * row key token is to be hashed 
	 * @param token the pre-defined row key token configuration
	 * @return the token value
	 */
	public byte[] getPredefinedFieldValueBytes(
			DataObject dataObject, Hash hash, 
			PredefinedField token) {
		PlasmaType rootType = (PlasmaType)dataObject.getType();
		
		byte[] result = null;
		switch (token.getName()) {
		case URI: 
			result = rootType.getURIBytes();
			break;
		case TYPE:
			QName qname = rootType.getQualifiedName();
			
			result = rootType.getPhysicalNameBytes();
			if (result == null || result.length == 0) {
				if (log.isDebugEnabled())
				    log.debug("no physical name for type, "
				    		+ qname.getNamespaceURI() + "#" + rootType.getName() 
				    		+ ", defined - using logical name");
				result = rootType.getNameBytes();
			}
			break;
		case UUID:
			result = Bytes.toBytes(((PlasmaDataObject)dataObject).getUUIDAsString());
			break;
		default:
		    throw new CloudGraphConfigurationException("invalid row key token name, "
		    		+ token.getName().name() + " - cannot get this token from a Data Graph");
		}
		
		if (token.isHash()) {
			int hashValue = hash.hash(result);
			result = Bytes.toBytes(String.valueOf(hashValue));
		}
		
		return result;
	}
}
