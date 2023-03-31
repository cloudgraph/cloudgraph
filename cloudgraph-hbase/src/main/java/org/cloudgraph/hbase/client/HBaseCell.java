package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.Cell;

public class HBaseCell implements Cell {
  private org.apache.hadoop.hbase.Cell cell;

  @SuppressWarnings("unused")
  private HBaseCell() {
  }

  public HBaseCell(org.apache.hadoop.hbase.Cell cell) {
    super();
    this.cell = cell;
  }

  public org.apache.hadoop.hbase.Cell get() {
    return cell;
  }

  public byte[] getFamily() {
    return cell.getFamily();
  }

  public byte[] getFamilyArray() {
    return cell.getFamilyArray();
  }

  public int getFamilyLength() {
    return cell.getFamilyLength();
  }

  public int getFamilyOffset() {
    return cell.getFamilyOffset();
  }

  public long getMvccVersion() {
    return cell.getMvccVersion();
  }

  public byte[] getQualifier() {
    return cell.getQualifier();
  }

  public byte[] getQualifierArray() {
    return cell.getQualifierArray();
  }

  public int getQualifierLength() {
    return cell.getQualifierLength();
  }

  public int getQualifierOffset() {
    return cell.getQualifierOffset();
  }

  public byte[] getRow() {
    return cell.getRow();
  }

  public byte[] getRowArray() {
    return cell.getRowArray();
  }

  public int getRowLength() {
    return cell.getRowLength();
  }

  public int getRowOffset() {
    return cell.getRowOffset();
  }

  public long getSequenceId() {
    return cell.getSequenceId();
  }

  public byte[] getTagsArray() {
    return cell.getTagsArray();
  }

  public int getTagsLength() {
    return cell.getTagsLength();
  }

  public int getTagsOffset() {
    return cell.getTagsOffset();
  }

  public long getTimestamp() {
    return cell.getTimestamp();
  }

  public byte getTypeByte() {
    return cell.getTypeByte();
  }

  public byte[] getValue() {
    return cell.getValue();
  }

  public byte[] getValueArray() {
    return cell.getValueArray();
  }

  public int getValueLength() {
    return cell.getValueLength();
  }

  public int getValueOffset() {
    return cell.getValueOffset();
  }

}
