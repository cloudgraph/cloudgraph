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
package org.cloudgraph.rdb.service;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.helper.DataConverter;

import commonj.sdo.Property;
import commonj.sdo.Type;

public class RDBDataConverter {
  private static Log log = LogFactory.getFactory().getInstance(RDBDataConverter.class);

  static public volatile RDBDataConverter INSTANCE = initializeInstance();
  private Map<Integer, String> sqlTypeMap = new HashMap<Integer, String>();

  private RDBDataConverter() {

    // Get all field in java.sql.Types
    Field[] fields = java.sql.Types.class.getFields();
    for (int i = 0; i < fields.length; i++) {
      try {
        String name = fields[i].getName();
        Integer value = (Integer) fields[i].get(null);
        sqlTypeMap.put(value, name);
      } catch (IllegalAccessException e) {
      }
    }
  }

  private static synchronized RDBDataConverter initializeInstance() {
    if (INSTANCE == null)
      INSTANCE = new RDBDataConverter();
    return INSTANCE;
  }

  public Object fromJDBCDataType(ResultSet rs, int columnIndex, int sourceType,
      PlasmaProperty targetProperty) throws SQLException {

    Object result = convertFrom(rs, columnIndex, sourceType, targetProperty);
    return result;
  }

  public int toJDBCDataType(PlasmaProperty sourceProperty, Object value) throws SQLException {

    int result = convertToSqlType(sourceProperty, value);
    ;
    return result;
  }

  public Object toJDBCDataValue(PlasmaProperty sourceProperty, Object value) throws SQLException {

    Object result = convertToSqlValue(sourceProperty, value);

    return result;
  }

  public DataFlavor toJDBCDataFlavor(PlasmaProperty sourceProperty) {
    if (!sourceProperty.getType().isDataType())
      throw new IllegalArgumentException("expected data type property, not "
          + sourceProperty.toString());
    PlasmaProperty dataProperty = sourceProperty;
    return dataProperty.getDataFlavor();
  }

  private Object convertToSqlValue(Property property, Object value) throws SQLException {

    if (!property.getType().isDataType())
      throw new IllegalArgumentException("expected data type property, not " + property.toString());
    DataType dataType = DataType.valueOf(property.getType().getName());
    Object result;
    switch (dataType) {
    case String:
    case URI:
    case Month:
    case MonthDay:
    case Day:
    case Time:
    case Year:
    case YearMonth:
    case YearMonthDay:
    case Duration:
      result = DataConverter.INSTANCE.toString(property.getType(), value);
      break;
    case Date:
      // Plasma SDO allows more precision than just month/day/year
      // in an SDO date datatype, and using java.sql.Date will
      // truncate
      // here so use java.sql.Timestamp.
      Date date = DataConverter.INSTANCE.toDate(property.getType(), value);
      result = new java.sql.Timestamp(date.getTime());
      break;
    case DateTime:
      date = DataConverter.INSTANCE.toDate(property.getType(), value);
      result = new java.sql.Timestamp(date.getTime());
      break;
    case Decimal:
      result = DataConverter.INSTANCE.toDecimal(property.getType(), value);
      break;
    case Bytes:
      result = DataConverter.INSTANCE.toBytes(property.getType(), value);
      break;
    case Byte:
      result = DataConverter.INSTANCE.toByte(property.getType(), value);
      break;
    case Boolean:
      result = DataConverter.INSTANCE.toBoolean(property.getType(), value);
      break;
    case Character:
      result = DataConverter.INSTANCE.toString(property.getType(), value);
      break;
    case Double:
      result = DataConverter.INSTANCE.toDouble(property.getType(), value);
      break;
    case Float:
      result = DataConverter.INSTANCE.toDouble(property.getType(), value);
      break;
    case Int:
      result = DataConverter.INSTANCE.toInt(property.getType(), value);
      break;
    case Integer:
      result = DataConverter.INSTANCE.toInteger(property.getType(), value);
      break;
    case Long:
      result = DataConverter.INSTANCE.toLong(property.getType(), value);
      break;
    case Short:
      result = DataConverter.INSTANCE.toShort(property.getType(), value);
      break;
    case Strings:
      result = DataConverter.INSTANCE.toString(property.getType(), value);
      break;
    case Object:
    default:
      result = DataConverter.INSTANCE.toString(property.getType(), value);
      break;
    }

    return result;
  }

  private int convertToSqlType(Property property, Object value) {
    int result;
    if (!property.getType().isDataType())
      throw new IllegalArgumentException("expected data type property, not " + property.toString());
    DataType dataType = DataType.valueOf(property.getType().getName());
    switch (dataType) {
    case String:
    case URI:
    case Month:
    case MonthDay:
    case Day:
    case Time:
    case Year:
    case YearMonth:
    case YearMonthDay:
    case Duration:
    case Strings:
      result = java.sql.Types.VARCHAR;
      break;
    case Date:
      // Plasma SDO allows more precision than just month/day/year
      // in an SDO date datatype, and using java.sql.Date will
      // truncate
      // here so use java.sql.Timestamp.
      result = java.sql.Types.TIMESTAMP;
      break;
    case DateTime:
      result = java.sql.Types.TIMESTAMP;
      // FIXME: so what SDO datatype maps to a SQL timestamp??
      break;
    case Decimal:
      result = java.sql.Types.DECIMAL;
      break;
    case Bytes:
      // FIXME: how do we know whether a Blob here
      result = java.sql.Types.VARBINARY;
      break;
    case Byte:
      result = java.sql.Types.VARBINARY;
      break;
    case Boolean:
      result = java.sql.Types.BOOLEAN;
      break;
    case Character:
      result = java.sql.Types.CHAR;
      break;
    case Double:
      result = java.sql.Types.DOUBLE;
      break;
    case Float:
      result = java.sql.Types.FLOAT;
      break;
    case Int:
      result = java.sql.Types.INTEGER;
      break;
    case Integer:
      result = java.sql.Types.BIGINT;
      break;
    case Long:
      result = java.sql.Types.INTEGER; // FIXME: no JDBC long??
      break;
    case Short:
      result = java.sql.Types.SMALLINT;
      break;
    case Object:
    default:
      result = java.sql.Types.VARCHAR;
      break;
    }
    return result;
  }

  private Object convertFrom(ResultSet rs, int columnIndex, int sourceType, Property property)
      throws SQLException {
    Object result = null;
    if (!property.getType().isDataType())
      throw new IllegalArgumentException("expected data type property, not " + property.toString());
    DataType targetDataType = DataType.valueOf(property.getType().getName());
    switch (targetDataType) {
    case String:
    case URI:
    case Month:
    case MonthDay:
    case Day:
    case Time:
    case Year:
    case YearMonth:
    case YearMonthDay:
    case Duration:
      result = rs.getString(columnIndex);
      break;
    case Date:
      java.sql.Timestamp ts = rs.getTimestamp(columnIndex);
      if (ts != null)
        result = new java.util.Date(ts.getTime());
      break;
    case DateTime:
      ts = rs.getTimestamp(columnIndex);
      if (ts != null) {
        // format DateTime String for SDO
        java.util.Date date = new java.util.Date(ts.getTime());
        result = DataConverter.INSTANCE.getDateTimeFormat().format(date);
      }
      break;
    case Decimal:
      result = rs.getBigDecimal(columnIndex);
      break;
    case Bytes:
      if (sourceType != Types.BLOB) {
        result = rs.getBytes(columnIndex);
      } else if (sourceType == Types.BLOB) {
        Blob blob = rs.getBlob(columnIndex);
        if (blob != null) {
          long blobLen = blob.length(); // for debugging
          // Note: blob.getBytes(columnIndex, blob.length()); is
          // somehow truncating the array
          // by something like 14 bytes (?!!) even though
          // blob.length() returns the expected length
          // using getBinaryStream which is preferred anyway
          InputStream is = blob.getBinaryStream();
          try {
            byte[] bytes = IOUtils.toByteArray(is);
            long len = bytes.length; // for debugging
            result = bytes;
          } catch (IOException e) {
            throw new RDBServiceException(e);
          } finally {
            try {
              is.close();
            } catch (IOException e) {
              log.error(e.getMessage(), e);
            }
          }
        }
      }
      break;
    case Byte:
      result = rs.getByte(columnIndex);
      break;
    case Boolean:
      result = rs.getBoolean(columnIndex);
      break;
    case Character:
      result = rs.getInt(columnIndex);
      break;
    case Double:
      result = rs.getDouble(columnIndex);
      break;
    case Float:
      result = rs.getFloat(columnIndex);
      break;
    case Int:
      result = rs.getInt(columnIndex);
      break;
    case Integer:
      result = new BigInteger(rs.getString(columnIndex));
      break;
    case Long:
      result = rs.getLong(columnIndex);
      break;
    case Short:
      result = rs.getShort(columnIndex);
      break;
    case Strings:
      String value = rs.getString(columnIndex);
      if (value != null) {
        String[] values = value.split("\\s");
        List<String> list = new ArrayList<String>(values.length);
        for (int i = 0; i < values.length; i++)
          list.add(values[i]); // what no Java 5 sugar for this ??
        result = list;
      }
      break;
    case Object:
    default:
      result = rs.getObject(columnIndex);
      break;
    }
    return result;
  }

  public String toJDBCString(Type sourceType, PlasmaProperty sourceProperty, Object value) {

    String result = null;
    DataFlavor flavor = sourceProperty.getDataFlavor();

    switch (flavor) {
    case integral:
    case real:
      result = value.toString();
      break;
    case string:
      result = "'" + value.toString() + "'";
      break;
    default:
      result = value.toString();
    }

    return result;
  }

  public String getJdbcTypeName(int jdbcType) {

    return sqlTypeMap.get(jdbcType);
  }
}
