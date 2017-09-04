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
package org.cloudgraph.hbase.io;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.key.CompositeColumnKeyFactory;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.plasma.sdo.PlasmaType;

/**
 * Local column qualifier/value map which is mutable such that column results
 * can be incrementally added. Intended for read operations not write as cell
 * versions are not accounted for. This class is not thread safe, use it in a
 * single threaded context only.
 * <p>
 * </p>
 * While the HBase client Result structure is great and extremely flexible, it
 * is not something we want to incrementally add qualifiers, values to during
 * repeated read operations, and not something we can create from data in
 * memory.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CellValues {

  private String rowKey;
  // Note: byte[] uses object identity for equals and hashCode
  // so can't be an effective map key
  private Map<String, Map<String, byte[]>> familyMap;
  /**
   * Whether the complete graph selection for context type is found within the
   * cell values
   */
  boolean completeSelection = false;

  @SuppressWarnings("unused")
  private CellValues() {
  }

  public CellValues(Result row) {
    this.rowKey = Bytes.toString(row.getRow());
    this.familyMap = new HashMap<>();
    addCells(row);
  }

  public CellValues(String rowKey) {
    this.rowKey = rowKey;
    this.familyMap = new HashMap<>();
  }

  public boolean isCompleteSelection() {
    return completeSelection;
  }

  public void setCompleteSelection(boolean completeSelection) {
    this.completeSelection = completeSelection;
  }

  public void addAll(Result row) {
    if (!this.rowKey.equals(Bytes.toString(row.getRow())))
      throw new IllegalArgumentException("row key mismatch('" + Bytes.toString(row.getRow())
          + "') - expected '" + this.rowKey + "'");
    addCells(row);
  }

  private void addCells(Result row) {
    String fam = null;
    for (Cell cell : row.listCells()) {
      fam = Bytes.toString(CellUtil.cloneFamily(cell));
      Map<String, byte[]> map = this.familyMap.get(fam);
      if (map == null) {
        map = new HashMap<>();
        this.familyMap.put(fam, map);
      }
      map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil.cloneValue(cell));
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + rowKey.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CellValues other = (CellValues) obj;
    if (rowKey == null) {
      if (other.rowKey != null)
        return false;
    } else if (!rowKey.equals(other.rowKey))
      return false;
    return true;
  }

  public void clear() {
    this.familyMap.clear();
  }

  public String getRowKey() {
    return rowKey;
  }

  public byte[] getRowKeyAsBytes() {
    return Bytes.toBytes(rowKey);
  }

  public void addColumn(KeyValue keyValue) {
    String fam = Bytes.toString(CellUtil.cloneFamily(keyValue));
    Map<String, byte[]> map = this.familyMap.get(fam);
    if (map == null) {
      map = new HashMap<>();
      this.familyMap.put(fam, map);
    }
    map.put(Bytes.toString(CellUtil.cloneQualifier(keyValue)), CellUtil.cloneValue(keyValue));
  }

  public void addColumn(byte[] family, byte[] qual, byte[] value) {
    String fam = Bytes.toString(family);
    Map<String, byte[]> map = this.familyMap.get(fam);
    if (map == null) {
      map = new HashMap<>();
      this.familyMap.put(fam, map);
    }
    map.put(Bytes.toString(qual), value);
  }

  public boolean containsColumn(byte[] family, byte[] qual) {
    String fam = Bytes.toString(family);
    Map<String, byte[]> map = this.familyMap.get(fam);
    return map != null && map.containsKey(Bytes.toString(qual));
  }

  public byte[] getColumnValue(byte[] family, byte[] qual) {
    String fam = Bytes.toString(family);
    Map<String, byte[]> map = this.familyMap.get(fam);
    if (map != null)
      return map.get(Bytes.toString(qual));
    else
      return null;
  }

  public String toString() {
    StringBuilder buf = new StringBuilder();
    // buf.append("map: ");
    Iterator<String> famIter = this.familyMap.keySet().iterator();
    for (int i = 0; famIter.hasNext(); i++) {
      String fam = famIter.next();
      buf.append(fam);
      buf.append(": ");
      Map<String, byte[]> map = this.familyMap.get(fam);
      Iterator<String> iter = map.keySet().iterator();
      for (int j = 0; iter.hasNext(); j++) {
        buf.append("\n\t");
        String qual = iter.next();
        byte[] value = map.get(qual);
        buf.append(qual);
        // buf.append("\t");
        // buf.append(Bytes.toString(value));
      }
    }
    return buf.toString();
  }
}
