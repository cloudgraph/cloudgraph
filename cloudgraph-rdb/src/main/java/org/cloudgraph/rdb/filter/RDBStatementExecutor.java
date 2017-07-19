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
package org.cloudgraph.rdb.filter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rdb.service.RDBDataConverter;
import org.cloudgraph.store.lang.StatementExecutor;
import org.cloudgraph.store.lang.StatementUtil;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.profile.KeyType;

import commonj.sdo.Property;

public class RDBStatementExecutor implements StatementExecutor {
	
    private static Log log = LogFactory.getFactory().getInstance(RDBStatementExecutor.class);
	protected RDBDataConverter converter = RDBDataConverter.INSTANCE;
	private Connection con;
	private StatementUtil statementUtil;
	
	@SuppressWarnings("unused")
	private RDBStatementExecutor() {}
	public RDBStatementExecutor(Connection con) {
		this.con = con;
		this.statementUtil = new StatementUtil();
	}
	@Override
	public List<List<PropertyPair>> fetch(PlasmaType type, StringBuilder sql) {
		return fetch(type, sql, new HashSet<Property>(), new Object[0]);
	}
	@Override
	public List<List<PropertyPair>> fetch(PlasmaType type, StringBuilder sql,
			Set<Property> props) {
		return fetch(type, sql, props, new Object[0]);
	}
	@Override
	public List<List<PropertyPair>> fetch(PlasmaType type, StringBuilder sql,
			Set<Property> props, Object[] params) {
		List<List<PropertyPair>> result = new ArrayList<List<PropertyPair>>();
        PreparedStatement statement = null;
        ResultSet rs = null; 
        try {
            if (log.isDebugEnabled() ){
                if (params == null || params.length == 0) {
                    log.debug("fetch: "+ sql.toString());                	
                }
                else
                {
                    StringBuilder paramBuf = new StringBuilder();
                	paramBuf.append(" [");
                    for (int p = 0; p < params.length; p++)
                    {
                        if (p > 0)
                        	paramBuf.append(", ");
                        paramBuf.append(String.valueOf(params[p]));
                    }
                    paramBuf.append("]");
                    log.debug("fetch: "+ sql.toString() 
                    		+ " " + paramBuf.toString());
                }
            } 
            statement = con.prepareStatement(sql.toString(),
               		ResultSet.TYPE_FORWARD_ONLY,/*ResultSet.TYPE_SCROLL_INSENSITIVE,*/
                    ResultSet.CONCUR_READ_ONLY);
		
            for (int i = 0; i < params.length; i++)
            	statement.setString(i+1, // FIXME
            			String.valueOf(params[i]));
            
            statement.execute();
            rs = statement.getResultSet();
            ResultSetMetaData rsMeta = rs.getMetaData();
            int numcols = rsMeta.getColumnCount();
            
            int count = 0;
            while (rs.next()) {
            	List<PropertyPair> row = new ArrayList<PropertyPair>(numcols);
            	result.add(row);
            	for(int i=1;i<=numcols;i++) {
            		String columnName = rsMeta.getColumnName(i);
            		int columnType = rsMeta.getColumnType(i);
            		PlasmaProperty prop = (PlasmaProperty)type.getProperty(columnName);
            		PlasmaProperty valueProp = prop;
			    	while (!valueProp.getType().isDataType()) {
			    		valueProp = this.statementUtil.getOppositePriKeyProperty(valueProp);
			    	}
              		Object value = converter.fromJDBCDataType(rs, 
            				i, columnType, valueProp);
            		if (value != null) {
            		    PropertyPair pair = new PropertyPair(
            			    (PlasmaProperty)prop, value);
            		    if (!valueProp.equals(prop))
            		    	pair.setValueProp(valueProp);
            		    if (!props.contains(prop))
            		    	pair.setQueryProperty(false);
            		    row.add(pair);
            		}
                }
            	count++;
            }
            if (log.isDebugEnabled())
                log.debug("returned "+ count + " results");
        }
        catch (Throwable t) {
            throw new DataAccessException(t);
        }
        finally {
			try {
	        	if (rs != null)
				    rs.close();
	        	if (statement != null)
				    statement.close();
			} catch (SQLException e) {
				log.error(e.getMessage(), e);
			}
        }
        return result;
	}
	@Override
	public Map<String, PropertyPair> fetchRowMap(PlasmaType type,
			StringBuilder sql) {
		Map<String, PropertyPair> result = new HashMap<String, PropertyPair>();
        PreparedStatement statement = null;
        ResultSet rs = null; 
        try {
            if (log.isDebugEnabled() ){
                log.debug("fetch: " + sql.toString());
            } 
            
            statement = con.prepareStatement(sql.toString(),
               		ResultSet.TYPE_FORWARD_ONLY,/*ResultSet.TYPE_SCROLL_INSENSITIVE,*/
                    ResultSet.CONCUR_READ_ONLY);
		
            statement.execute();
            rs = statement.getResultSet();
            ResultSetMetaData rsMeta = rs.getMetaData();
            int numcols = rsMeta.getColumnCount();
            int count = 0;
            while (rs.next()) {
            	for(int i=1;i<=numcols;i++) {
            		String columnName = rsMeta.getColumnName(i);
            		int columnType = rsMeta.getColumnType(i);
            		PlasmaProperty prop = (PlasmaProperty)type.getProperty(columnName);
            		PlasmaProperty valueProp = prop;
			    	while (!valueProp.getType().isDataType()) {
			    		valueProp = this.statementUtil.getOppositePriKeyProperty(valueProp);
			    	}
              		Object value = converter.fromJDBCDataType(rs, 
            				i, columnType, valueProp);
            		if (value != null) {
            		    PropertyPair pair = new PropertyPair(
            			    (PlasmaProperty)prop, value);
            		    if (!valueProp.equals(prop))
            		    	pair.setValueProp(valueProp);
            		    result.put(prop.getName(), pair);
            		}
                }
            	count++;
            }
            if (log.isDebugEnabled())
                log.debug("returned "+ count + " results");
        }
        catch (Throwable t) {
            throw new DataAccessException(t);
        }
        finally {
			try {
	        	if (rs != null)
				    rs.close();
	        	if (statement != null)
				    statement.close();
			} catch (SQLException e) {
				log.error(e.getMessage(), e);
			}
        }
        return result;
	}
	@Override
	public List<PropertyPair> fetchRow(PlasmaType type, StringBuilder sql) {
		List<PropertyPair> result = new ArrayList<PropertyPair>();
        PreparedStatement statement = null;
        ResultSet rs = null; 
        try {
            if (log.isDebugEnabled() ){
                log.debug("fetch: " + sql.toString());
            } 
            statement = con.prepareStatement(sql.toString(),
               		ResultSet.TYPE_FORWARD_ONLY,/*ResultSet.TYPE_SCROLL_INSENSITIVE,*/
                    ResultSet.CONCUR_READ_ONLY);
		            
            statement.execute();
            rs = statement.getResultSet();
            ResultSetMetaData rsMeta = rs.getMetaData();
            int numcols = rsMeta.getColumnCount();
            int count = 0;
            while (rs.next()) {
            	for(int i=1;i<=numcols;i++) {
            		String columnName = rsMeta.getColumnName(i);
            		int columnType = rsMeta.getColumnType(i);
            		PlasmaProperty prop = (PlasmaProperty)type.getProperty(columnName);
            		PlasmaProperty valueProp = prop;
			    	while (!valueProp.getType().isDataType()) {
			    		valueProp = this.statementUtil.getOppositePriKeyProperty(valueProp);
			    	}
              		Object value = converter.fromJDBCDataType(rs, 
            				i, columnType, valueProp);
            		if (value != null) {
            		    PropertyPair pair = new PropertyPair(
            			    (PlasmaProperty)prop, value);
            		    if (!valueProp.equals(prop))
            		    	pair.setValueProp(valueProp);
            		    result.add(pair);
            		}
                }
            	count++;
            }
            if (log.isDebugEnabled())
                log.debug("returned "+ count + " results");
        }
        catch (Throwable t) {
            throw new DataAccessException(t);
        }
        finally {
			try {
	        	if (rs != null)
				    rs.close();
	        	if (statement != null)
				    statement.close();
			} catch (SQLException e) {
				log.error(e.getMessage(), e);
			}
        }
        return result;
	}
	@Override
	public void execute(PlasmaType type, StringBuilder sql,
			Map<String, PropertyPair> values) {
        PreparedStatement statement = null;
        List<InputStream> streams = null;
        try {		
            if (log.isDebugEnabled() ){
                log.debug("execute: " + sql.toString());
                StringBuilder paramBuf = createParamDebug(values);
                log.debug("params: " + paramBuf.toString());
            } 
            statement = con.prepareStatement(sql.toString());
    		for (PropertyPair pair : values.values()) {
    			PlasmaProperty valueProp = pair.getProp();
    			if (pair.getValueProp() != null)
    				valueProp = pair.getValueProp();
    			 
    			int jdbcType = converter.toJDBCDataType(valueProp, pair.getValue());
    			Object jdbcValue = converter.toJDBCDataValue(valueProp, pair.getValue());
    			if (jdbcType != Types.BLOB && jdbcType != Types.VARBINARY) {
    			    statement.setObject(pair.getColumn(), 
    					jdbcValue, jdbcType);
    			}
    			else {
    				byte[] bytes = (byte[])jdbcValue;
    				long len = bytes.length;
    				ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        			statement.setBinaryStream(pair.getColumn(), is, len); 
        			if (streams == null)
        				streams = new ArrayList<InputStream>();
        			streams.add(is);
    			} 
    			
    			if (pair.getOldValue() != null) {
        			Object jdbcOldValue = converter.toJDBCDataValue(valueProp, pair.getOldValue());
        			if (jdbcType != Types.BLOB && jdbcType != Types.VARBINARY) {
        			    statement.setObject(pair.getOldValueColumn(), 
        			    		jdbcOldValue, jdbcType);
        			}
        			else {
        				byte[] bytes = (byte[])jdbcOldValue;
        				long len = bytes.length;
        				ByteArrayInputStream is = new ByteArrayInputStream(bytes);
            			statement.setBinaryStream(pair.getOldValueColumn(), is, len); 
            			if (streams == null)
            				streams = new ArrayList<InputStream>();
            			streams.add(is);
        			} 
    			}
    		}
            statement.executeUpdate();
        }
        catch (Throwable t) {
            throw new DataAccessException(t);
        }
        finally {
			try {
	        	if (statement != null)
				    statement.close();
			} catch (SQLException e) {
				log.error(e.getMessage(), e);
			}
			if (streams != null)
				try {
					for (InputStream stream : streams)
					    stream.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
        }
	}
	
	@Override
	public void executeInsert(PlasmaType type, StringBuilder sql,
			Map<String, PropertyPair> values) {
        PreparedStatement statement = null;
        List<InputStream> streams = null;
        try {
		
            if (log.isDebugEnabled() ){
                log.debug("execute: " + sql.toString());
                StringBuilder paramBuf = createParamDebug(values);
                log.debug("params: " + paramBuf.toString());
            } 
             
            statement = con.prepareStatement(sql.toString());
            
    		for (PropertyPair pair : values.values()) {
    			PlasmaProperty valueProp = pair.getProp();
    			if (pair.getValueProp() != null)
    				valueProp = pair.getValueProp();
    			int jdbcType = converter.toJDBCDataType(valueProp, pair.getValue());
    			Object jdbcValue = converter.toJDBCDataValue(valueProp, pair.getValue());
    			if (jdbcType != Types.BLOB && jdbcType != Types.VARBINARY) {
    			    statement.setObject(pair.getColumn(), 
    					jdbcValue, jdbcType);
    			}
    			else {
    				byte[] bytes = (byte[])jdbcValue;
    				long len = bytes.length;
    				ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        			statement.setBinaryStream(pair.getColumn(), is, len);    				
        			if (streams == null)
        				streams = new ArrayList<InputStream>();
        			streams.add(is);
    			}    			
    		}
            
            statement.execute();
        }
        catch (Throwable t) {
            throw new DataAccessException(t);
        }
        finally {
			try {
	        	if (statement != null)
				    statement.close();
			} catch (SQLException e) {
				log.error(e.getMessage(), e);
			}
			if (streams != null)
				try {
					for (InputStream stream : streams)
					    stream.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
					
        }
	}	
 
	private StringBuilder createParamDebug(Map<String, PropertyPair> values) throws SQLException {
		StringBuilder paramBuf = new StringBuilder();
        paramBuf.append("[");
        paramBuf.append("[");
		int i = 1;
		for (PropertyPair pair : values.values()) {
			PlasmaProperty valueProp = pair.getProp();
			if (pair.getValueProp() != null)
				valueProp = pair.getValueProp();
			int jdbcType = converter.toJDBCDataType(valueProp, pair.getValue());
			Object jdbcValue = converter.toJDBCDataValue(valueProp, pair.getValue());
			Object jdbcOldValue = null;
			if (pair.getOldValue() != null) 
				jdbcOldValue = converter.toJDBCDataValue(valueProp, pair.getOldValue());
			 
        	if (i > 1) {
        		paramBuf.append(", ");
        	}
        	paramBuf.append("(");
        	paramBuf.append(jdbcValue.getClass().getSimpleName());
        	paramBuf.append("/");
        	paramBuf.append(converter.getJdbcTypeName(jdbcType));
        	paramBuf.append(")");
        	paramBuf.append(String.valueOf(jdbcValue));
        	if (jdbcOldValue != null) {
        		paramBuf.append("(");
        		paramBuf.append(String.valueOf(jdbcOldValue));
        		paramBuf.append(")");
        	}
			i++;		
		}
    	paramBuf.append("]");
		return paramBuf;
	}
	@Override
	public List<PropertyPair> executeInsertWithGeneratedKeys(PlasmaType type,
			StringBuilder sql, Map<String, PropertyPair> values) {
		List<PropertyPair> resultKeys = new ArrayList<PropertyPair>();
        PreparedStatement statement = null;
        List<InputStream> streams = null;
        ResultSet generatedKeys = null;
        try {
		
            if (log.isDebugEnabled() ){
                log.debug("execute: " + sql.toString());
                StringBuilder paramBuf = createParamDebug(values);
                log.debug("params: " + paramBuf.toString());
            } 
             
            statement = con.prepareStatement(sql.toString(), 
            		PreparedStatement.RETURN_GENERATED_KEYS);
            
    		for (PropertyPair pair : values.values()) {
    			PlasmaProperty valueProp = pair.getProp();
    			if (pair.getValueProp() != null)
    				valueProp = pair.getValueProp();
    			int jdbcType = converter.toJDBCDataType(valueProp, pair.getValue());
    			Object jdbcValue = converter.toJDBCDataValue(valueProp, pair.getValue());
    			if (jdbcType != Types.BLOB && jdbcType != Types.VARBINARY) {
    			    statement.setObject(pair.getColumn(), 
    					jdbcValue, jdbcType);
    			}
    			else {
    				byte[] bytes = (byte[])jdbcValue;
    				long len = bytes.length;
    				ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        			statement.setBinaryStream(pair.getColumn(), is, len);    				
        			if (streams == null)
        				streams = new ArrayList<InputStream>();
        			streams.add(is);
   			    }    			
    		}
            
            statement.execute();
            generatedKeys = statement.getGeneratedKeys();
            ResultSetMetaData rsMeta = generatedKeys.getMetaData();
            int numcols = rsMeta.getColumnCount();
            if (log.isDebugEnabled())
            	log.debug("returned " + numcols + " keys");
            
            if (generatedKeys.next()) {
                // FIXME; without metadata describing which properties
            	// are actually a sequence, there is guess work
            	// involved in matching the values returned
            	// automatically from PreparedStatment as they
            	// are anonymous in terms of the column names
            	// making it impossible to match them to a metadata
            	// property. 
            	List<Property> pkPropList = type.findProperties(KeyType.primary);
                if (pkPropList == null || pkPropList.size() == 0)
                    throw new DataAccessException("no pri-key properties found for type '" 
                            + type.getName() + "'");
                if (pkPropList.size() > 1)
                    throw new DataAccessException("multiple pri-key properties found for type '" 
                            + type.getName() + "' - cannot map to generated keys");
                PlasmaProperty prop = (PlasmaProperty)pkPropList.get(0);
                //FIXME: need to find properties per column by physical name alias
                // in case where multiple generated pri-keys
            	for(int i=1; i<=numcols; i++) {
            		String columnName = rsMeta.getColumnName(i);
                    if (log.isDebugEnabled())
                    	log.debug("returned key column '" + columnName + "'");
            		int columnType = rsMeta.getColumnType(i);
              		Object value = converter.fromJDBCDataType(generatedKeys, 
            				i, columnType, prop);
        		    PropertyPair pair = new PropertyPair(
            			    (PlasmaProperty)prop, value);
            		resultKeys.add(pair);
                }
            }
        }
        catch (Throwable t) {
            throw new DataAccessException(t);
        }
        finally {
			try {
	        	if (statement != null)
				    statement.close();
			} catch (SQLException e) {
				log.error(e.getMessage(), e);
			}
			if (streams != null)
				try {
					for (InputStream stream : streams)
					    stream.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
        }
        
        return resultKeys;
	}

}
