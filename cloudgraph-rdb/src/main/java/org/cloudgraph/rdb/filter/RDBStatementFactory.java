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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rdb.service.RDBDataConverter;
import org.cloudgraph.store.lang.FilterAssembler;
import org.cloudgraph.store.lang.GraphFilterException;
import org.cloudgraph.store.lang.StatementFactory;
import org.cloudgraph.store.lang.StatementUtil;
import org.cloudgraph.store.service.AliasMap;
import org.plasma.config.DataAccessProviderName;
import org.plasma.config.PlasmaConfig;
import org.plasma.config.RDBMSVendorName;
import org.plasma.query.model.Where;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.profile.ConcurrencyType;
import org.plasma.sdo.profile.ConcurrentDataFlavor;
import org.plasma.sdo.profile.KeyType;

import commonj.sdo.Property;
import commonj.sdo.Type;

public class RDBStatementFactory implements StatementFactory {
	
    private static Log log = LogFactory.getFactory().getInstance(RDBStatementFactory.class);
	protected RDBDataConverter converter = RDBDataConverter.INSTANCE;
	private StatementUtil statementUtil;
	
	public RDBStatementFactory() {
		this.statementUtil = new StatementUtil();
	}

	@Override
	public StringBuilder createSelectConcurrent(PlasmaType type,
			List<PropertyPair> keyValues, int waitSeconds, List<Object> params) {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");		
		List<Property> props = new ArrayList<Property>();
		for (PropertyPair pair : keyValues)
			props.add(pair.getProp());
		
		Property lockingUserProperty = type.findProperty(ConcurrencyType.pessimistic, 
            	ConcurrentDataFlavor.user);
        if (lockingUserProperty != null)
        	props.add(lockingUserProperty);
        else
            if (log.isDebugEnabled())
                log.debug("could not find locking user property for type, "
                    + type.getURI() + "#" + type.getName());  
        
        Property lockingTimestampProperty = type.findProperty(ConcurrencyType.pessimistic, 
            	ConcurrentDataFlavor.time);
        if (lockingTimestampProperty != null)
        	props.add(lockingTimestampProperty);
        else
            if (log.isDebugEnabled())
                log.debug("could not find locking timestamp property for type, "
                    + type.getURI() + "#" + type.getName());  

        Property concurrencyUserProperty = type.findProperty(ConcurrencyType.optimistic, 
        	ConcurrentDataFlavor.user);
        if (concurrencyUserProperty != null)
        	props.add(concurrencyUserProperty);
        else
            if (log.isDebugEnabled())
                log.debug("could not find optimistic concurrency (username) property for type, "
                    + type.getURI() + "#" + type.getName());          
        
        Property concurrencyTimestampProperty = type.findProperty(ConcurrencyType.optimistic, 
        	ConcurrentDataFlavor.time);
        if (concurrencyTimestampProperty != null)
        	props.add(concurrencyTimestampProperty);
        else
            if (log.isDebugEnabled())
                log.debug("could not find optimistic concurrency timestamp property for type, "
                    + type.getURI() + "#" + type.getName());  
				
		int i = 0;
		for (Property p : props) {
			PlasmaProperty prop = (PlasmaProperty)p;
			if (prop.isMany() && !prop.getType().isDataType())
				continue;
			if (i > 0)
				sql.append(", ");
			sql.append("t0.");
			sql.append(prop.getPhysicalName());
			i++;
		}
		sql.append(" FROM ");
		sql.append(this.statementUtil.getQualifiedPhysicalName(type));
		sql.append(" t0 ");
		sql.append(" WHERE ");
		try {
	        for (int k = 0; k < keyValues.size(); k++) {
	        	if (k > 0)
	        		sql.append(" AND ");
	        	PropertyPair propValue = keyValues.get(k);
	        	sql.append("t0.");  
	        	sql.append(propValue.getProp().getPhysicalName());
	        	sql.append(" = "); 
	        	this.appendValue(propValue, true, sql);
	        }
		}
		catch (SQLException e) {
			throw new GraphFilterException(e);
		}
        RDBMSVendorName vendor = PlasmaConfig.getInstance().getRDBMSProviderVendor(DataAccessProviderName.JDBC);
        switch (vendor) {
        case ORACLE:
            sql.append(" FOR UPDATE WAIT ");
            sql.append(String.valueOf(waitSeconds));
	        break;
        case MYSQL:
            sql.append(" FOR UPDATE");
	        break;
	    default:
            sql.append(" FOR UPDATE WAIT");
            sql.append(String.valueOf(waitSeconds));
        }

		return sql;
	}

	@Override
	public StringBuilder createSelect(PlasmaType type, Set<Property> props,
			List<PropertyPair> keyValues, List<Object> params) {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");		
		
		int count = 0;
		// always select pk props where not found in given list
		List<Property> pkProps = type.findProperties(KeyType.primary);
		for (Property pkProp : pkProps) {
			if (props.contains(pkProp))  
				continue;
			if (count > 0)
				sql.append(", ");
			sql.append("t0.");
			sql.append(((PlasmaProperty)pkProp).getPhysicalName());			
			count++;
		}
		
		for (Property p : props) {
			PlasmaProperty prop = (PlasmaProperty)p;
			if (prop.isMany() && !prop.getType().isDataType())
				continue;
			if (count > 0)
				sql.append(", ");
			sql.append("t0.");
			sql.append(prop.getPhysicalName());
			count++;
		}		
		
		sql.append(" FROM ");
		sql.append(this.statementUtil.getQualifiedPhysicalName(type));
		sql.append(" t0 ");
		sql.append(" WHERE ");
		try {
	        for (count = 0; count < keyValues.size(); count++) {
	        	if (count > 0)
	        		sql.append(" AND ");
	        	PropertyPair propValue = keyValues.get(count);
	        	sql.append("t0.");  
	        	sql.append(propValue.getProp().getPhysicalName());
	        	sql.append(" = ?"); 
	        	params.add(this.getParamValue(propValue));
	        }
		}
		catch (SQLException e) {
			throw new GraphFilterException(e);
		}
		
		return sql;
	}

	@Override
	public StringBuilder createSelect(PlasmaType type, Set<Property> props,
			List<PropertyPair> keyValues, FilterAssembler filterAssembler,
			List<Object> params) {
		StringBuilder sql = new StringBuilder();
		AliasMap aliasMap = ((RDBFilterAssembler)filterAssembler).getAliasMap();
		sql.append("SELECT ");		
		int count = 0;
		// always select pk props where not found in given list
		List<Property> pkProps = type.findProperties(KeyType.primary);
		for (Property pkProp : pkProps) {
			if (props.contains(pkProp))
				continue;
			if (count > 0)
				sql.append(", ");
			sql.append("t0.");
			sql.append(((PlasmaProperty)pkProp).getPhysicalName());			
			count++;
		}
		for (Property p : props) {
			PlasmaProperty prop = (PlasmaProperty)p;
			if (prop.isMany() && !prop.getType().isDataType())
				continue;
			if (count > 0)
				sql.append(", ");
			sql.append("t0.");
			sql.append(prop.getPhysicalName());
			count++;
		}
		sql.append(" FROM ");
    	Iterator<PlasmaType> it = aliasMap.getTypes();
    	count = 0;
    	while (it.hasNext()) {
    		PlasmaType aliasType = it.next();
    		String alias = aliasMap.getAlias(aliasType); 
    		if (count > 0)
    			sql.append(", ");
    		sql.append(this.statementUtil.getQualifiedPhysicalName(aliasType));
    		sql.append(" ");
    		sql.append(alias);
    		count++;
    	}
    	sql.append(" ");
    	sql.append(filterAssembler.getFilter());
    	for (Object filterParam : filterAssembler.getParams())
    		params.add(filterParam);
    	try {
	        for (count = 0; count < keyValues.size(); count++) {
	            sql.append(" AND ");
	        	PropertyPair propValue = keyValues.get(count);
	        	sql.append("t0.");  
	        	sql.append(propValue.getProp().getPhysicalName());
	        	sql.append(" = ?"); 
	        	params.add(this.getParamValue(propValue));
	        }
		}
		catch (SQLException e) {
			throw new GraphFilterException(e);
		}
        
        // add default ordering by given keys
    	sql.append(" ORDER BY ");
        for (count = 0; count < keyValues.size(); count++) {
        	if (count > 0)        		
                sql.append(", ");
        	PropertyPair propValue = keyValues.get(count);
        	sql.append("t0.");  
        	sql.append(propValue.getProp().getPhysicalName());
        }
		
		return sql;
	}

	@Override
	public StringBuilder createInsert(PlasmaType type,
			Map<String, PropertyPair> values) {
		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO ");
		sql.append(this.statementUtil.getQualifiedPhysicalName(type));
		sql.append("(");
		int i = 0;
		for (PropertyPair pair : values.values()) {
			PlasmaProperty prop = pair.getProp();
			if (prop.isMany() && !prop.getType().isDataType())
				continue;
			if (i > 0)
				sql.append(", ");
			sql.append(pair.getProp().getPhysicalName());
			pair.setColumn(i+1);
			i++;
		}
		sql.append(") VALUES (");
		
		i = 0;
		for (PropertyPair pair : values.values()) {
			PlasmaProperty prop = pair.getProp();
			if (prop.isMany() && !prop.getType().isDataType())
				continue;
			if (i > 0)
				sql.append(", ");
			sql.append("?");
			i++;
		}
		sql.append(")");
		return sql;
	}

	@Override
	public StringBuilder createUpdate(PlasmaType type,
			Map<String, PropertyPair> values) {
		StringBuilder sql = new StringBuilder();
		
		// construct an 'update' for all non pri-keys and
		// excluding many reference properties
		sql.append("UPDATE ");		
		sql.append(this.statementUtil.getQualifiedPhysicalName(type));
		sql.append(" t0 SET ");
		int col = 0;
		for (PropertyPair pair : values.values()) {
			PlasmaProperty prop = pair.getProp();
			if (prop.isMany() && !prop.getType().isDataType())
				continue;
			if (prop.isKey(KeyType.primary)) {
				if (pair.getOldValue() == null) // key not modified
				    continue; // ignore keys here
			}
			if (col > 0)
				sql.append(", ");
			sql.append("t0.");
			sql.append(prop.getPhysicalName());
        	sql.append(" = ?"); 
        	pair.setColumn(col+1);
			col++;
		}
        // construct a 'where' continuing to append parameters
		// for each pri-key
		int key = 0;
		sql.append(" WHERE ");
		for (PropertyPair pair : values.values()) {
			PlasmaProperty prop = pair.getProp();
			if (prop.isMany() && !prop.getType().isDataType())
				continue;
			if (!prop.isKey(KeyType.primary))
				continue;
			if (key > 0)
				sql.append(" AND ");  
        	sql.append("t0.");  
        	sql.append(pair.getProp().getPhysicalName());
        	sql.append(" = ?"); 
        	if (pair.getOldValue() == null) // key not modified
        	    pair.setColumn(col+1);
        	else
        		pair.setOldValueColumn(col+1);
        	col++; 
        	key++;
        }
		
		return sql;
	}

	@Override
	public StringBuilder createDelete(PlasmaType type,
			Map<String, PropertyPair> values) {
		StringBuilder sql = new StringBuilder();
		sql.append("DELETE FROM ");		
		sql.append(this.statementUtil.getQualifiedPhysicalName(type));
		sql.append(" WHERE ");
		int i = 0;
		for (PropertyPair pair : values.values()) {
			PlasmaProperty prop = pair.getProp();
			if (prop.isMany() && !prop.getType().isDataType())
				continue;
			if (!prop.isKey(KeyType.primary))
				continue;
        	if (i > 0)
        		sql.append(" AND "); 
        	sql.append(pair.getProp().getPhysicalName());
        	sql.append(" = ? "); 
        	pair.setColumn(i+1);
        	i++;
        }
		
		return sql;
	}

	@Override
	public PlasmaProperty getOppositePriKeyProperty(Property targetProperty) {
		return this.statementUtil.getOppositePriKeyProperty(targetProperty);
	}

	@Override
	public boolean hasUpdatableProperties(Map<String, PropertyPair> values) {
		return this.statementUtil.hasUpdatableProperties(values);
	}
	
	@Override
	public FilterAssembler createFilterAssembler(Where where, Type targetType) {
		 AliasMap aliasMap = new AliasMap((PlasmaType)targetType);
		 return new RDBFilterAssembler(where, targetType, aliasMap);
	}

	private Object getParamValue(PropertyPair pair) throws SQLException  
	{
		PlasmaProperty valueProp = pair.getProp();
		if (pair.getValueProp() != null)
			valueProp = pair.getValueProp();
		
    	Object jdbcValue = RDBDataConverter.INSTANCE.toJDBCDataValue(valueProp, 
    			pair.getValue());
    	DataFlavor dataFlavor = RDBDataConverter.INSTANCE.toJDBCDataFlavor(valueProp);
    	
    	switch (dataFlavor) {
    	case string:
    	case temporal:
    	case other:
    	    break;
    	default:
     	   break;
    	}	
    	
    	return jdbcValue;
	}	
	
	private void appendValue(PropertyPair pair, boolean useOldValue, StringBuilder sql) throws SQLException
	{
		PlasmaProperty valueProp = pair.getProp();
		if (pair.getValueProp() != null)
			valueProp = pair.getValueProp();
		
		Object jdbcValue = null;
		if (!useOldValue || pair.getOldValue() == null)
    	    jdbcValue = RDBDataConverter.INSTANCE.toJDBCDataValue(valueProp, 
    			pair.getValue());
		else
    	    jdbcValue = RDBDataConverter.INSTANCE.toJDBCDataValue(valueProp, 
    			pair.getOldValue());
    	
    	DataFlavor dataFlavor = RDBDataConverter.INSTANCE.toJDBCDataFlavor(valueProp);
    	
    	switch (dataFlavor) {
    	case string:
    	case temporal:
    	case other:
    	    sql.append("'");
    	    sql.append(jdbcValue);
    	    sql.append("'");
    	    break;
    	default:
    	    sql.append(jdbcValue);
     	   break;
    	}		
	}

}
