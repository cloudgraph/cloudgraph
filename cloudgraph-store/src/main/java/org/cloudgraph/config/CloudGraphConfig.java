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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.xml.bind.JAXBException;
import javax.xml.bind.UnmarshalException;
import javax.xml.namespace.QName;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.common.env.EnvProperties;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.helper.PlasmaTypeHelper;
import org.xml.sax.SAXException;

import commonj.sdo.Type;


/**
 * Configuration marshaling and un-marshaling with 
 * data access convenience methods. Looks for the Java
 * command line '-Dcloudgraph.configuration' setting for the
 * name of the configuration file. If not found looks for
 * the default file name 'cloudgraph-config.xml'. The CloudGraph
 * configuration file must be somewhere on the Java class path.     
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CloudGraphConfig implements Config   {

    private static Log log = LogFactory.getLog(CloudGraphConfig.class);
    private static volatile CloudGraphConfig instance = null;
    private static final String PROPERTY_NAME_CLOUDGRAPH_CONFIG = "cloudgraph.configuration";     
    private static final String DEFAULT_CONFIG_FILE_NAME = "cloudgraph-config.xml";     
    private static final String DEFAULT_PROPERTIES_FILE_NAME = "cloudgraph.properties";     
    
    private CloudGraphConfiguration config;
    private ConfigProperties configProperties;
    private Charset charset = Charset.forName( CoreConstants.UTF8_ENCODING );
    
    private Map<QName, TableConfig> graphURIToTableMap = new HashMap<QName, TableConfig>();
    private Map<QName, DataGraphConfig> graphURIToGraphMap = new HashMap<QName, DataGraphConfig>();
    private Map<String, TableConfig> tableNameToTableMap = new HashMap<String, TableConfig>();
    private Map<String, Property> propertyNameToPropertyMap = new HashMap<String, Property>();
    
    private ReadWriteLock lock = new ReentrantReadWriteLock();
        
    private CloudGraphConfig()
    {
        log.debug("initializing...");
		try {
			configProperties = new ConfigProperties();
			configProperties.addConfiguration(new SystemConfiguration());
			
	        InputStream propertiesStream = CloudGraphConfiguration.class.getResourceAsStream(DEFAULT_PROPERTIES_FILE_NAME);
	        if (propertiesStream == null)
	        	propertiesStream = CloudGraphConfig.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTIES_FILE_NAME);
	        if (propertiesStream != null) {			
			    configProperties.addConfiguration(new PropertiesConfiguration(DEFAULT_PROPERTIES_FILE_NAME));
	        }
		}
		catch (ConfigurationException e) {
			throw new CloudGraphConfigurationException(e);
		}

		try {
            
            String fileName = EnvProperties.instance().getProperty(
            		PROPERTY_NAME_CLOUDGRAPH_CONFIG);
            
            if (fileName == null)
                fileName = DEFAULT_CONFIG_FILE_NAME;
            
            CloudGraphConfigDataBinding configBinding = new CloudGraphConfigDataBinding(
	        		new CloudGraphConfigValidationEventHandler());
	        
            config = unmarshalConfig(fileName, configBinding);
            
            for (Property prop : config.getProperties())
            	propertyNameToPropertyMap.put(prop.getName(), prop);
            
            for (Table table : config.tables) {
            	TableConfig tableConfig = new TableConfig(table, this);
            	mapTable(tableConfig);
            }
        }
        catch (SAXException e) {
            throw new CloudGraphConfigurationException(e);
        }
        catch (JAXBException e) {
            throw new CloudGraphConfigurationException(e);
        }
        
    }  
    
    private void mapTable(TableConfig tableConfig) {
		if (this.tableNameToTableMap.get(tableConfig.getQualifiedName()) != null)
			throw new CloudGraphConfigurationException("a table definition already exists for qualified name '"
					+ tableConfig.getQualifiedName() + "'");
    	this.tableNameToTableMap.put(tableConfig.getQualifiedName(), tableConfig);
    	for (DataGraph graph : tableConfig.getTable().getDataGraphs()) {    		
    		DataGraphConfig dataGraphConfig = new DataGraphConfig(graph, tableConfig);
    		mapDataGraph(dataGraphConfig);
    	}    	
    }
    
    private void unmapTable(TableConfig tableConfig) {
		if (this.tableNameToTableMap.get(tableConfig.getQualifiedName()) == null)
			throw new CloudGraphConfigurationException("table definition does not exist exists for qualified name '"
					+ tableConfig.getQualifiedName() + "'");
    	
    	for (DataGraph graph : tableConfig.getTable().getDataGraphs()) {    		
    		DataGraphConfig dataGraphConfig = new DataGraphConfig(graph, tableConfig);
    		unmapDataGraph(dataGraphConfig);
    	} 
    	
    	this.tableNameToTableMap.remove(tableConfig.getQualifiedName());
    }
    
    private void mapDataGraph(DataGraphConfig dataGraphConfig) {
		
		QName qname = new QName(dataGraphConfig.getGraph().getUri(), dataGraphConfig.getGraph().getType());
		PlasmaType configuredType = (PlasmaType)PlasmaTypeHelper.INSTANCE.getType(qname.getNamespaceURI(), 
				qname.getLocalPart());
		//if (configuredType.isAbstract())
		//	throw new CloudGraphConfigurationException("a data graph definition within table '"
		//			+ table.getName() + "' has an abstract type (uri/name), " 
		//			+ graph.getUri() + "#" + graph.getType() + " - use a non abstract type");
		if (graphURIToTableMap.get(qname) != null)
			throw new CloudGraphConfigurationException("a data graph definition already exists within table '"
					+ dataGraphConfig.getTable().getTable().getName() + "' for type (uri/name), " 
					+ dataGraphConfig.getGraph().getUri() + "#" + dataGraphConfig.getGraph().getType());
		graphURIToTableMap.put(qname, dataGraphConfig.getTable());
		graphURIToGraphMap.put(qname, dataGraphConfig);
		/*
		Map<QName, PlasmaType> hierarchy = new HashMap<QName, PlasmaType>();
		this.collectTypeHierarchy(configuredType, hierarchy);
		
		for (PlasmaType type : hierarchy.values()) {
			qname = type.getQualifiedName();
    		if (graphURIToTableMap.get(qname) != null)
    			throw new CloudGraphConfigurationException("a data graph definition already exists within table '"
    					+ table.getName() + "' for type (uri/name), " 
    					+ graph.getUri() + "#" + graph.getType());
    		graphURIToTableMap.put(qname, tableConfig);
    		graphURIToGraphMap.put(qname, dataGraphConfig);
		}
		*/    	
    }
 
    private void unmapDataGraph(DataGraphConfig dataGraphConfig) {
		
		QName qname = new QName(dataGraphConfig.getGraph().getUri(), dataGraphConfig.getGraph().getType());
		if (graphURIToTableMap.get(qname) == null)
			throw new CloudGraphConfigurationException("no data graph definition already exists within table '"
					+ dataGraphConfig.getTable().getTable().getName() + "' for type (uri/name), " 
					+ dataGraphConfig.getGraph().getUri() + "#" + dataGraphConfig.getGraph().getType());
		graphURIToTableMap.remove(qname);
    }
    
    @SuppressWarnings("unchecked")
    private CloudGraphConfiguration unmarshalConfig(String configFileName, CloudGraphConfigDataBinding binding)
    {
    	try {
	        InputStream stream = CloudGraphConfiguration.class.getResourceAsStream(configFileName);
	        if (stream == null)
	            stream = CloudGraphConfig.class.getClassLoader().getResourceAsStream(configFileName);
	        if (stream == null)
	            throw new CloudGraphConfigurationException("could not find configuration file resource '" 
	                    + configFileName 
	                    + "' on the current classpath");        
	        
	        CloudGraphConfiguration result = (CloudGraphConfiguration)binding.validate(stream);
            return result;
    	}
        catch (UnmarshalException e) {
            throw new CloudGraphConfigurationException(e);
        }
        catch (JAXBException e) {
            throw new CloudGraphConfigurationException(e);
        }
    }
    
	/* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#marshal(java.io.OutputStream)
	 */
	@Override
	public void marshal(OutputStream stream) {
        try {
        	CloudGraphConfigDataBinding configBinding = new CloudGraphConfigDataBinding(
                    new DefaultValidationEventHandler());
            configBinding.marshal(this.config, stream);
        } catch (JAXBException e1) {
            throw new CloudGraphConfigurationException(e1);
        } catch (SAXException e1) {
            throw new CloudGraphConfigurationException(e1);
        }
    }
    
    public static Config getInstance()
        throws CloudGraphConfigurationException
    {
        if (instance == null)
            initializeInstance();
        return instance;
    }
    
    private static synchronized void initializeInstance()
    {
        if (instance == null)
            instance = new CloudGraphConfig();
    }
    
	@Override
    public ConfigProperties getConfigProperties() {
		return configProperties;
	}

	/* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#getProperties()
	 */
    @Override
	public List<Property> getProperties() {
        return config.properties;
    } 
    
    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#findProperty(java.lang.String)
	 */
    @Override
	public Property findProperty(String name) {
    	return this.propertyNameToPropertyMap.get(name);
    }
    
    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#findTable(javax.xml.namespace.QName)
	 */
    @Override
	public TableConfig findTable(QName typeName) {
    	PlasmaType type = (PlasmaType)PlasmaTypeHelper.INSTANCE.getType(typeName.getNamespaceURI(), 
    			typeName.getLocalPart());
		return this.graphURIToTableMap.get(
				type.getQualifiedName());
    }
    
    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#getTable(javax.xml.namespace.QName)
	 */
    @Override
	public TableConfig getTable(QName typeName) {
    	TableConfig result = findTable(typeName);
    	if (result == null)
    		throw new CloudGraphConfigurationException("no HTable configured for " +
    				" graph URI '" + typeName.toString() + "'");
    	return result;
    }

    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#findTable(commonj.sdo.Type)
	 */
    @Override
	public TableConfig findTable(Type type) {
		return this.graphURIToTableMap.get(
				((PlasmaType)type).getQualifiedName());
    }
    
    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#getTable(commonj.sdo.Type)
	 */
    @Override
	public TableConfig getTable(Type type) {
    	TableConfig result = findTable(type);
    	if (result == null)
    		throw new CloudGraphConfigurationException("no HTable configured for " +
    				" graph URI '" + ((PlasmaType)type).getQualifiedName() + "'");
    	return result;
    }
    
    public void addTable() {
    	
    }
    
	private void collectTypeHierarchy(PlasmaType type, Map<QName, PlasmaType> map) {
		map.put(type.getQualifiedName(), type);
		// get ancestry
		collectBaseTypes(type, map);
		Collection<PlasmaType> coll = map.values();
		PlasmaType[] types = new PlasmaType[coll.size()];
		coll.toArray(types);
		
		// get all derived type for every ancestor
		for (int i = 0; i < types.length; i++) {
			PlasmaType baseType = types[i];
			collectSubTypes(baseType, map);
		}		
    }
	
	private void collectBaseTypes(PlasmaType type, Map<QName, PlasmaType> map) {
		for (Type t : type.getBaseTypes()) {
			PlasmaType baseType = (PlasmaType)t;
			map.put(baseType.getQualifiedName(), baseType);
			collectBaseTypes(baseType, map);
		}
	}
	
	private void collectSubTypes(PlasmaType type, Map<QName, PlasmaType> map) {
		for (Type t : type.getSubTypes()) {
			PlasmaType subType = (PlasmaType)t;
			map.put(subType.getQualifiedName(), subType);
			collectSubTypes(subType, map);
		}
 	}
    
    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#findTable(java.lang.String)
	 */
    @Override
	public TableConfig findTable(String tableName) {
    	TableConfig result = this.tableNameToTableMap.get(tableName);
    	return result;
    }

    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#getTable(java.lang.String)
	 */
    @Override
	public TableConfig getTable(String tableNamespace, String tableName) {
    	TableConfig result = this.tableNameToTableMap.get(tableName);
    	if (result == null)
    		throw new CloudGraphConfigurationException("no table configured for" +
    				" name '" + tableName.toString() + "'");
    	return result;
    }
    
    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#getTableName(javax.xml.namespace.QName)
	 */
    @Override
	public String getTableName(QName typeName) {
    	TableConfig result = this.graphURIToTableMap.get(typeName);
    	if (result == null)
    		throw new CloudGraphConfigurationException("no HTable configured for" +
    				" CloudGraph '" + typeName.toString() + "'");
    	return result.getQualifiedName();
    }

	@Override
	public void addTable(TableConfig tableConfig) {
		 lock.writeLock().lock();
		 try {
			 this.mapTable(tableConfig);
		 }
		 finally {
			 lock.writeLock().unlock();
		 }
	}  
	
	@Override
	public void removeTable(TableConfig tableConfig) {
		 lock.writeLock().lock();
		 try {
			 this.unmapTable(tableConfig);
		 }
		 finally {
			 lock.writeLock().unlock();
		 }
	}  
	    
    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#findDataGraph(javax.xml.namespace.QName)
	 */
    @Override
	public DataGraphConfig findDataGraph(QName qname) {
    	DataGraphConfig result = this.graphURIToGraphMap.get(qname);
    	return result;
    }

    /* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#getDataGraph(javax.xml.namespace.QName)
	 */
    @Override
	public DataGraphConfig getDataGraph(QName qname) {
    	DataGraphConfig result = this.graphURIToGraphMap.get(qname);
    	if (result == null)
    		throw new CloudGraphConfigurationException("no configured for" +
    				" '" + qname.toString() + "'");
    	return result;
    }

	/* (non-Javadoc)
	 * @see org.cloudgraph.config.TableMapping#getCharset()
	 */
	@Override
	public Charset getCharset() {
		return charset;
	}

	private Boolean uniqueChecksVar = null;
	@Override
	public boolean uniqueChecks() {
		if (uniqueChecksVar == null) {
			uniqueChecksVar = getTablePropertyBoolean(ConfigurationProperty.CLOUDGRAPH___UNIQUE___CHECKS, 
					this.config.isUniqueChecks(), true);
		}
		return this.uniqueChecksVar.booleanValue();
	}

	private Boolean tombstoneRowsVar = null;
	@Override
	public boolean tombstoneRows() {
		if (tombstoneRowsVar == null) {
			tombstoneRowsVar = getTablePropertyBoolean(ConfigurationProperty.CLOUDGRAPH___TOMBSTONE___ROWS, 
					this.config.isTombstoneRows(), true);
		}
		return this.tombstoneRowsVar.booleanValue();
	}
	
	private Boolean tombstoneRowsOverwriteableVar = null;
	@Override
	public boolean tombstoneRowsOverwriteable() {
		if (tombstoneRowsOverwriteableVar == null) {
			tombstoneRowsOverwriteableVar = getTablePropertyBoolean(ConfigurationProperty.CLOUDGRAPH___TOMBSTONE___ROWS___OVERWRITEABLE, 
					this.config.isTombstoneRowsOverwriteable(), false);
		}
		return this.tombstoneRowsOverwriteableVar.booleanValue();
	}
	
	private String maprdbTablePathPrefixVar = null;
	@Override
	public String maprdbTablePathPrefix() {
		if (maprdbTablePathPrefixVar == null) {
			maprdbTablePathPrefixVar = getTablePropertyString(ConfigurationProperty.CLOUDGRAPH___MAPRDB___TABLE___PATH___PREFIX, 
					this.config.getMaprdbTablePathPrefix(), null);
		}
		return this.maprdbTablePathPrefixVar;
	}	

	/**
	 * Checks for system property, then value changed at table level, then global config
	 * level. 
	 * @param prop the config prop
	 * @param tableValue the current table value
	 * @param defaultValue the default for the property. 
	 * @return the String value
	 */
	private String getTablePropertyString(ConfigurationProperty prop, 
			String tableValue, String defaultValue) {
		String result;

		String value = this.getConfigProperties().getString(prop.value());
		if (value != null) {
			result = value;
		}
		else {		
			if (defaultValue != tableValue) { // default overridden, take the table value
				result = tableValue;	
			}
			else { // check for global config prop
				Property globalProp = this.findProperty(prop.value()); 
				if (globalProp != null) {
					result = globalProp.getValue();
				}
				else { // otherwise use schema default
					result = defaultValue;
				}
			}		 
		}
	    return result;
	}
	
	/**
	 * Checks for system property, then value changed at table level, then global config
	 * level. 
	 * @param prop the config prop
	 * @param tableValue the current table value
	 * @param defaultValue the default for the property. 
	 * @return the boolean value
	 */
	private Boolean getTablePropertyBoolean(ConfigurationProperty prop, 
			boolean tableValue, boolean defaultValue) {
		Boolean result;

		String value = this.getConfigProperties().getString(prop.value());
		if (value != null) {
			result = Boolean.valueOf(value);
		}
		else {		
			if (defaultValue != tableValue) { // default overridden, take the table value
				result = Boolean.valueOf(tableValue);	
			}
			else { // check for global config prop
				Property globalProp = this.findProperty(prop.value()); 
				if (globalProp != null) {
					result = Boolean.valueOf(globalProp.getValue());
				}
				else { // otherwise use schema default
					result = Boolean.valueOf(defaultValue);
				}	
			}
		}
	    return result;
	}
}
