/**
 * Copyright 2017 TerraMeta Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudgraph.rdb.service;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rdb.filter.RDBFilterAssembler;
import org.cloudgraph.store.service.AliasMap;
import org.plasma.runtime.DataAccessProviderName;
import org.plasma.runtime.PlasmaRuntime;
import org.plasma.runtime.RDBMSVendorName;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.profile.ConcurrencyType;
import org.plasma.sdo.profile.ConcurrentDataFlavor;
import org.plasma.sdo.profile.KeyType;

import commonj.sdo.Property;

@Deprecated
public abstract class JDBCSupport {

  private static Log log = LogFactory.getFactory().getInstance(JDBCSupport.class);
  protected RDBDataConverter converter = RDBDataConverter.INSTANCE;

  protected JDBCSupport() {

  }

  protected StringBuilder createSelectForUpdate(PlasmaType type, List<PropertyPair> keyValues,
      int waitSeconds) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT ");
    List<Property> props = new ArrayList<Property>();
    for (PropertyPair pair : keyValues)
      props.add(pair.getProp());

    Property lockingUserProperty = type.findProperty(ConcurrencyType.pessimistic,
        ConcurrentDataFlavor.user);
    if (lockingUserProperty != null)
      props.add(lockingUserProperty);
    else if (log.isDebugEnabled())
      log.debug("could not find locking user property for type, " + type.getURI() + "#"
          + type.getName());

    Property lockingTimestampProperty = type.findProperty(ConcurrencyType.pessimistic,
        ConcurrentDataFlavor.time);
    if (lockingTimestampProperty != null)
      props.add(lockingTimestampProperty);
    else if (log.isDebugEnabled())
      log.debug("could not find locking timestamp property for type, " + type.getURI() + "#"
          + type.getName());

    Property concurrencyUserProperty = type.findProperty(ConcurrencyType.optimistic,
        ConcurrentDataFlavor.user);
    if (concurrencyUserProperty != null)
      props.add(concurrencyUserProperty);
    else if (log.isDebugEnabled())
      log.debug("could not find optimistic concurrency (username) property for type, "
          + type.getURI() + "#" + type.getName());

    Property concurrencyTimestampProperty = type.findProperty(ConcurrencyType.optimistic,
        ConcurrentDataFlavor.time);
    if (concurrencyTimestampProperty != null)
      props.add(concurrencyTimestampProperty);
    else if (log.isDebugEnabled())
      log.debug("could not find optimistic concurrency timestamp property for type, "
          + type.getURI() + "#" + type.getName());

    int i = 0;
    for (Property p : props) {
      PlasmaProperty prop = (PlasmaProperty) p;
      if (prop.isMany() && !prop.getType().isDataType())
        continue;
      if (i > 0)
        sql.append(", ");
      sql.append("t0.");
      sql.append(prop.getPhysicalName());
      i++;
    }
    sql.append(" FROM ");
    sql.append(getQualifiedPhysicalName(type));
    sql.append(" t0 ");
    sql.append(" WHERE ");
    for (int k = 0; k < keyValues.size(); k++) {
      if (k > 0)
        sql.append(" AND ");
      PropertyPair propValue = keyValues.get(k);
      sql.append("t0.");
      sql.append(propValue.getProp().getPhysicalName());
      sql.append(" = ");
      appendValue(propValue, true, sql);
    }
    RDBMSVendorName vendor = PlasmaRuntime.getInstance().getRDBMSProviderVendor(
        DataAccessProviderName.JDBC);
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

  protected String getQualifiedPhysicalName(PlasmaType type) {
    String packageName = type.getPackagePhysicalName();
    if (packageName != null)
      return packageName + "." + type.getPhysicalName();
    else
      return type.getPhysicalName();
  }

  protected StringBuilder createSelect(PlasmaType type, Set<Property> props,
      List<PropertyPair> keyValues, List<Object> params) throws SQLException {
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
      sql.append(((PlasmaProperty) pkProp).getPhysicalName());
      count++;
    }

    for (Property p : props) {
      PlasmaProperty prop = (PlasmaProperty) p;
      if (prop.isMany() && !prop.getType().isDataType())
        continue;
      if (count > 0)
        sql.append(", ");
      sql.append("t0.");
      sql.append(prop.getPhysicalName());
      count++;
    }

    sql.append(" FROM ");
    sql.append(getQualifiedPhysicalName(type));
    sql.append(" t0 ");
    sql.append(" WHERE ");
    for (count = 0; count < keyValues.size(); count++) {
      if (count > 0)
        sql.append(" AND ");
      PropertyPair propValue = keyValues.get(count);
      sql.append("t0.");
      sql.append(propValue.getProp().getPhysicalName());
      sql.append(" = ?");
      params.add(this.getParamValue(propValue));
      // appendValue(propValue, sql);
    }

    return sql;
  }

  protected StringBuilder createSelect(PlasmaType type, Set<Property> props,
      List<PropertyPair> keyValues, RDBFilterAssembler filterAssembler, List<Object> params,
      AliasMap aliasMap) throws SQLException {
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
      sql.append(((PlasmaProperty) pkProp).getPhysicalName());
      count++;
    }
    for (Property p : props) {
      PlasmaProperty prop = (PlasmaProperty) p;
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
      sql.append(getQualifiedPhysicalName(aliasType));
      sql.append(" ");
      sql.append(alias);
      count++;
    }
    sql.append(" ");
    sql.append(filterAssembler.getFilter());
    for (Object filterParam : filterAssembler.getParams())
      params.add(filterParam);
    for (count = 0; count < keyValues.size(); count++) {
      sql.append(" AND ");
      PropertyPair propValue = keyValues.get(count);
      sql.append("t0.");
      sql.append(propValue.getProp().getPhysicalName());
      sql.append(" = ?");
      params.add(this.getParamValue(propValue));
      // appendValue(propValue, sql);
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

  private void appendValue(PropertyPair pair, StringBuilder sql) throws SQLException {
    appendValue(pair, false, sql);
  }

  private void appendValue(PropertyPair pair, boolean useOldValue, StringBuilder sql)
      throws SQLException {
    PlasmaProperty valueProp = pair.getProp();
    if (pair.getValueProp() != null)
      valueProp = pair.getValueProp();

    Object jdbcValue = null;
    if (!useOldValue || pair.getOldValue() == null)
      jdbcValue = RDBDataConverter.INSTANCE.toJDBCDataValue(valueProp, pair.getValue());
    else
      jdbcValue = RDBDataConverter.INSTANCE.toJDBCDataValue(valueProp, pair.getOldValue());

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

  private Object getParamValue(PropertyPair pair) throws SQLException {
    PlasmaProperty valueProp = pair.getProp();
    if (pair.getValueProp() != null)
      valueProp = pair.getValueProp();

    Object jdbcValue = RDBDataConverter.INSTANCE.toJDBCDataValue(valueProp, pair.getValue());
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

  protected StringBuilder createInsert(PlasmaType type, Map<String, PropertyPair> values) {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ");
    sql.append(getQualifiedPhysicalName(type));
    sql.append("(");
    int i = 0;
    for (PropertyPair pair : values.values()) {
      PlasmaProperty prop = pair.getProp();
      if (prop.isMany() && !prop.getType().isDataType())
        continue;
      if (i > 0)
        sql.append(", ");
      sql.append(pair.getProp().getPhysicalName());
      pair.setColumn(i + 1);
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

  protected boolean hasUpdatableProperties(Map<String, PropertyPair> values) {

    for (PropertyPair pair : values.values()) {
      PlasmaProperty prop = pair.getProp();
      if (prop.isMany() && !prop.getType().isDataType())
        continue; // no such thing as updatable many reference property
      // in RDBMS
      if (prop.isKey(KeyType.primary))
        if (pair.getOldValue() == null) // key not modified, we're not
          // updating it
          continue;
      return true;
    }
    return false;
  }

  protected StringBuilder createUpdate(PlasmaType type, Map<String, PropertyPair> values) {
    StringBuilder sql = new StringBuilder();

    // construct an 'update' for all non pri-keys and
    // excluding many reference properties
    sql.append("UPDATE ");
    sql.append(getQualifiedPhysicalName(type));
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
      pair.setColumn(col + 1);
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
        pair.setColumn(col + 1);
      else
        pair.setOldValueColumn(col + 1);
      col++;
      key++;
    }

    return sql;
  }

  protected StringBuilder createDelete(PlasmaType type, Map<String, PropertyPair> values) {
    StringBuilder sql = new StringBuilder();
    sql.append("DELETE FROM ");
    sql.append(getQualifiedPhysicalName(type));
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
      pair.setColumn(i + 1);
      i++;
    }

    return sql;
  }

  protected List<List<PropertyPair>> fetch(PlasmaType type, StringBuilder sql, Connection con) {
    return fetch(type, sql, new HashSet<Property>(), new Object[0], con);
  }

  protected List<List<PropertyPair>> fetch(PlasmaType type, StringBuilder sql, Set<Property> props,
      Connection con) {
    return fetch(type, sql, props, new Object[0], con);
  }

  protected List<List<PropertyPair>> fetch(PlasmaType type, StringBuilder sql, Set<Property> props,
      Object[] params, Connection con) {
    List<List<PropertyPair>> result = new ArrayList<List<PropertyPair>>();
    PreparedStatement statement = null;
    ResultSet rs = null;
    try {
      if (log.isDebugEnabled()) {
        if (params == null || params.length == 0) {
          log.debug("fetch: " + sql.toString());
        } else {
          StringBuilder paramBuf = new StringBuilder();
          paramBuf.append(" [");
          for (int p = 0; p < params.length; p++) {
            if (p > 0)
              paramBuf.append(", ");
            paramBuf.append(String.valueOf(params[p]));
          }
          paramBuf.append("]");
          log.debug("fetch: " + sql.toString() + " " + paramBuf.toString());
        }
      }
      statement = con.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY,/*
                                                                                    * ResultSet
                                                                                    * .
                                                                                    * TYPE_SCROLL_INSENSITIVE
                                                                                    * ,
                                                                                    */
          ResultSet.CONCUR_READ_ONLY);

      for (int i = 0; i < params.length; i++)
        statement.setString(i + 1, // FIXME
            String.valueOf(params[i]));

      statement.execute();
      rs = statement.getResultSet();
      ResultSetMetaData rsMeta = rs.getMetaData();
      int numcols = rsMeta.getColumnCount();

      int count = 0;
      while (rs.next()) {
        List<PropertyPair> row = new ArrayList<PropertyPair>(numcols);
        result.add(row);
        for (int i = 1; i <= numcols; i++) {
          String columnName = rsMeta.getColumnName(i);
          int columnType = rsMeta.getColumnType(i);
          PlasmaProperty prop = (PlasmaProperty) type.getProperty(columnName);
          PlasmaProperty valueProp = prop;
          while (!valueProp.getType().isDataType()) {
            valueProp = getOppositePriKeyProperty(valueProp);
          }
          Object value = converter.fromJDBCDataType(rs, i, columnType, valueProp);
          if (value != null) {
            PropertyPair pair = new PropertyPair((PlasmaProperty) prop, value);
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
        log.debug("returned " + count + " results");
    } catch (Throwable t) {
      throw new DataAccessException(t);
    } finally {
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

  protected Map<String, PropertyPair> fetchRowMap(PlasmaType type, StringBuilder sql, Connection con) {
    Map<String, PropertyPair> result = new HashMap<String, PropertyPair>();
    PreparedStatement statement = null;
    ResultSet rs = null;
    try {
      if (log.isDebugEnabled()) {
        log.debug("fetch: " + sql.toString());
      }

      statement = con.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY,/*
                                                                                    * ResultSet
                                                                                    * .
                                                                                    * TYPE_SCROLL_INSENSITIVE
                                                                                    * ,
                                                                                    */
          ResultSet.CONCUR_READ_ONLY);

      statement.execute();
      rs = statement.getResultSet();
      ResultSetMetaData rsMeta = rs.getMetaData();
      int numcols = rsMeta.getColumnCount();
      int count = 0;
      while (rs.next()) {
        for (int i = 1; i <= numcols; i++) {
          String columnName = rsMeta.getColumnName(i);
          int columnType = rsMeta.getColumnType(i);
          PlasmaProperty prop = (PlasmaProperty) type.getProperty(columnName);
          PlasmaProperty valueProp = prop;
          while (!valueProp.getType().isDataType()) {
            valueProp = getOppositePriKeyProperty(valueProp);
          }
          Object value = converter.fromJDBCDataType(rs, i, columnType, valueProp);
          if (value != null) {
            PropertyPair pair = new PropertyPair((PlasmaProperty) prop, value);
            if (!valueProp.equals(prop))
              pair.setValueProp(valueProp);
            result.put(prop.getName(), pair);
          }
        }
        count++;
      }
      if (log.isDebugEnabled())
        log.debug("returned " + count + " results");
    } catch (Throwable t) {
      throw new DataAccessException(t);
    } finally {
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

  protected List<PropertyPair> fetchRow(PlasmaType type, StringBuilder sql, Connection con) {
    List<PropertyPair> result = new ArrayList<PropertyPair>();
    PreparedStatement statement = null;
    ResultSet rs = null;
    try {
      if (log.isDebugEnabled()) {
        log.debug("fetch: " + sql.toString());
      }
      statement = con.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY,/*
                                                                                    * ResultSet
                                                                                    * .
                                                                                    * TYPE_SCROLL_INSENSITIVE
                                                                                    * ,
                                                                                    */
          ResultSet.CONCUR_READ_ONLY);

      statement.execute();
      rs = statement.getResultSet();
      ResultSetMetaData rsMeta = rs.getMetaData();
      int numcols = rsMeta.getColumnCount();
      int count = 0;
      while (rs.next()) {
        for (int i = 1; i <= numcols; i++) {
          String columnName = rsMeta.getColumnName(i);
          int columnType = rsMeta.getColumnType(i);
          PlasmaProperty prop = (PlasmaProperty) type.getProperty(columnName);
          PlasmaProperty valueProp = prop;
          while (!valueProp.getType().isDataType()) {
            valueProp = getOppositePriKeyProperty(valueProp);
          }
          Object value = converter.fromJDBCDataType(rs, i, columnType, valueProp);
          if (value != null) {
            PropertyPair pair = new PropertyPair((PlasmaProperty) prop, value);
            if (!valueProp.equals(prop))
              pair.setValueProp(valueProp);
            result.add(pair);
          }
        }
        count++;
      }
      if (log.isDebugEnabled())
        log.debug("returned " + count + " results");
    } catch (Throwable t) {
      throw new DataAccessException(t);
    } finally {
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

  protected void execute(PlasmaType type, StringBuilder sql, Map<String, PropertyPair> values,
      Connection con) {
    PreparedStatement statement = null;
    List<InputStream> streams = null;
    try {
      if (log.isDebugEnabled()) {
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
          statement.setObject(pair.getColumn(), jdbcValue, jdbcType);
        } else {
          byte[] bytes = (byte[]) jdbcValue;
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
            statement.setObject(pair.getOldValueColumn(), jdbcOldValue, jdbcType);
          } else {
            byte[] bytes = (byte[]) jdbcOldValue;
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
    } catch (Throwable t) {
      throw new DataAccessException(t);
    } finally {
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

  protected void executeInsert(PlasmaType type, StringBuilder sql,
      Map<String, PropertyPair> values, Connection con) {
    PreparedStatement statement = null;
    List<InputStream> streams = null;
    try {

      if (log.isDebugEnabled()) {
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
          statement.setObject(pair.getColumn(), jdbcValue, jdbcType);
        } else {
          byte[] bytes = (byte[]) jdbcValue;
          long len = bytes.length;
          ByteArrayInputStream is = new ByteArrayInputStream(bytes);
          statement.setBinaryStream(pair.getColumn(), is, len);
          if (streams == null)
            streams = new ArrayList<InputStream>();
          streams.add(is);
        }
      }

      statement.execute();
    } catch (Throwable t) {
      throw new DataAccessException(t);
    } finally {
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

  protected List<PropertyPair> executeInsertWithGeneratedKeys(PlasmaType type, StringBuilder sql,
      Map<String, PropertyPair> values, Connection con) {
    List<PropertyPair> resultKeys = new ArrayList<PropertyPair>();
    PreparedStatement statement = null;
    List<InputStream> streams = null;
    ResultSet generatedKeys = null;
    try {

      if (log.isDebugEnabled()) {
        log.debug("execute: " + sql.toString());
        StringBuilder paramBuf = createParamDebug(values);
        log.debug("params: " + paramBuf.toString());
      }

      statement = con.prepareStatement(sql.toString(), PreparedStatement.RETURN_GENERATED_KEYS);

      for (PropertyPair pair : values.values()) {
        PlasmaProperty valueProp = pair.getProp();
        if (pair.getValueProp() != null)
          valueProp = pair.getValueProp();
        int jdbcType = converter.toJDBCDataType(valueProp, pair.getValue());
        Object jdbcValue = converter.toJDBCDataValue(valueProp, pair.getValue());
        if (jdbcType != Types.BLOB && jdbcType != Types.VARBINARY) {
          statement.setObject(pair.getColumn(), jdbcValue, jdbcType);
        } else {
          byte[] bytes = (byte[]) jdbcValue;
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
          throw new DataAccessException("no pri-key properties found for type '" + type.getName()
              + "'");
        if (pkPropList.size() > 1)
          throw new DataAccessException("multiple pri-key properties found for type '"
              + type.getName() + "' - cannot map to generated keys");
        PlasmaProperty prop = (PlasmaProperty) pkPropList.get(0);
        // FIXME: need to find properties per column by physical name
        // alias
        // in case where multiple generated pri-keys
        for (int i = 1; i <= numcols; i++) {
          String columnName = rsMeta.getColumnName(i);
          if (log.isDebugEnabled())
            log.debug("returned key column '" + columnName + "'");
          int columnType = rsMeta.getColumnType(i);
          Object value = converter.fromJDBCDataType(generatedKeys, i, columnType, prop);
          PropertyPair pair = new PropertyPair((PlasmaProperty) prop, value);
          resultKeys.add(pair);
        }
      }
    } catch (Throwable t) {
      throw new DataAccessException(t);
    } finally {
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

  protected PlasmaProperty getOppositePriKeyProperty(Property targetProperty) {
    PlasmaProperty opposite = (PlasmaProperty) targetProperty.getOpposite();
    PlasmaType oppositeType = null;

    if (opposite != null) {
      oppositeType = (PlasmaType) opposite.getContainingType();
    } else {
      oppositeType = (PlasmaType) targetProperty.getType();
    }

    List<Property> pkeyProps = oppositeType.findProperties(KeyType.primary);
    if (pkeyProps.size() == 0) {
      throw new DataAccessException("no opposite pri-key properties found"
          + " - cannot map from reference property, " + targetProperty.toString());
    }
    PlasmaProperty supplier = ((PlasmaProperty) targetProperty).getKeySupplier();
    if (supplier != null) {
      return supplier;
    } else if (pkeyProps.size() == 1) {
      return (PlasmaProperty) pkeyProps.get(0);
    } else {
      throw new DataAccessException("multiple opposite pri-key properties found"
          + " - cannot map from reference property, " + targetProperty.toString()
          + " - please add a derivation supplier");
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

}
