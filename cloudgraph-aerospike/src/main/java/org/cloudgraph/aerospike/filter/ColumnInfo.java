package org.cloudgraph.aerospike.filter;

import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

public class ColumnInfo {
  private String column;
  private byte[] columnBytes;
  private PlasmaType type;
  private PlasmaProperty property;

  public ColumnInfo(String column, byte[] columnBytes, PlasmaType type) {
    super();
    this.column = column;
    this.columnBytes = columnBytes;
    this.type = type;
  }

  public String getColumn() {
    return column;
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

  public void setProperty(PlasmaProperty property) {
    this.property = property;
  }

  public boolean hasProperty() {
    return property != null; // can be a meta column w/o a storage property
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("ColumnInfo [column=");
    buf.append(column);
    buf.append(", type=");
    buf.append(type);
    if (property != null) {
      buf.append(", property=");
      buf.append(property);
    }

    return buf.toString();
  }

}
