package org.cloudgraph.rocksdb.filter;

import org.cloudgraph.common.Bytes;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

public class ColumnInfo {
  private byte[] familyBytes;
  private byte[] columnBytes;
  private PlasmaType type;
  /** can be null */
  private PlasmaProperty property;
  /** can be null */
  private DataType dataType;
  private String columnName;

  @SuppressWarnings("unused")
  private ColumnInfo() {
  }

  public ColumnInfo(byte[] familyBytes, byte[] columnBytes, PlasmaType type, PlasmaProperty property) {
    super();
    this.familyBytes = familyBytes;
    this.columnName = Bytes.toString(columnBytes);
    this.columnBytes = columnBytes;
    this.type = type;
    this.property = property;
    if (this.type == null)
      throw new IllegalArgumentException("expected type arg");
    if (this.property == null)
      throw new IllegalArgumentException("expected property arg");
  }

  public ColumnInfo(byte[] familyBytes, byte[] columnBytes, DataType dataType) {
    super();
    this.familyBytes = familyBytes;
    this.columnName = Bytes.toString(columnBytes);
    this.columnBytes = columnBytes;
    this.dataType = dataType;
    if (this.dataType == null)
      throw new IllegalArgumentException("expected dataType arg");
  }

  public String getColumn() {
    return this.columnName;
  }

  public byte[] getColumnBytes() {
    return columnBytes;
  }

  public PlasmaType getType() {
    return type;
  }

  public PlasmaProperty getProperty() {
    return property;
  }

  // public void setProperty(PlasmaProperty property) {
  // this.property = property;
  // }

  public boolean hasProperty() {
    return property != null; // can be a meta column w/o a storage property
  }

  public boolean hasDataType() {
    return dataType != null; // can be a meta column w/o a storage property
  }

  public DataType getDataType() {
    return dataType;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("ColumnInfo [column=");
    buf.append(this.columnName);
    buf.append(", type=");
    buf.append(type);
    if (property != null) {
      buf.append(", property=");
      buf.append(property);
    }

    return buf.toString();
  }

}
