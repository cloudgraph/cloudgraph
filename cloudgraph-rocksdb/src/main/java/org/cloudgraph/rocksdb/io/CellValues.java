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
package org.cloudgraph.rocksdb.io;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.rocksdb.ext.Result;

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

  private byte[] rowKey;
  private Map<Integer, Map<Integer, byte[]>> familyMap;
  private Map<Integer, byte[]> reverseArrayHashMap;;
  /**
   * Whether the complete graph selection for context type is found within the
   * cell values
   */
  boolean completeSelection = false;

  @SuppressWarnings("unused")
  private CellValues() {
  }

  public CellValues(Result row) {
    this.rowKey = row.getRow();
    this.familyMap = new HashMap<>();
    this.reverseArrayHashMap = new HashMap<>();
    addCells(row);
  }

  public CellValues(byte[] rowKey) {
    this.rowKey = rowKey;
    this.familyMap = new HashMap<>();
    this.reverseArrayHashMap = new HashMap<>();
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
    List<KeyValue> list = row.list();
    for (KeyValue kv : list) {
      int famHash = Arrays.hashCode(kv.getFamily());
      this.reverseArrayHashMap.put(famHash, kv.getFamily());
      Map<Integer, byte[]> map = this.familyMap.get(famHash);
      if (map == null) {
        map = new HashMap<>();
        this.familyMap.put(famHash, map);
      }
      byte[] qual = kv.getQualifier();
      int qualHash = Arrays.hashCode(qual);
      map.put(qualHash, kv.getValue());
      this.reverseArrayHashMap.put(qualHash, qual);
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

  public byte[] getRowKey() {
    return rowKey;
  }

  public byte[] getRowKeyAsBytes() {
    return rowKey;
  }

  public void addColumn(KeyValue keyValue) {
    int famHash = Arrays.hashCode(keyValue.getFamily());
    this.reverseArrayHashMap.put(famHash, keyValue.getFamily());
    Map<Integer, byte[]> map = this.familyMap.get(famHash);
    if (map == null) {
      map = new HashMap<>();
      this.familyMap.put(famHash, map);
    }
    byte[] qual = keyValue.getQualifier();
    int qualHash = Arrays.hashCode(qual);
    map.put(qualHash, keyValue.getValue());
    this.reverseArrayHashMap.put(qualHash, qual);
  }

  public void addColumn(byte[] family, byte[] qual, byte[] value) {
    int famHash = Arrays.hashCode(family);
    this.reverseArrayHashMap.put(famHash, family);
    Map<Integer, byte[]> map = this.familyMap.get(famHash);
    if (map == null) {
      map = new HashMap<>();
      this.familyMap.put(famHash, map);
    }
    Integer qualHash = Arrays.hashCode(qual);
    map.put(qualHash, value);
    this.reverseArrayHashMap.put(qualHash, qual);
  }

  public boolean containsColumn(byte[] family, byte[] qual) {
    int fam = Arrays.hashCode(family);
    Map<Integer, byte[]> map = this.familyMap.get(fam);
    Integer qualHashCode = Arrays.hashCode(qual);
    return map != null && map.containsKey(qualHashCode);
  }

  public byte[] getColumnValue(byte[] family, byte[] qual) {
    int fam = Arrays.hashCode(family);
    Map<Integer, byte[]> map = this.familyMap.get(fam);
    if (map != null) {
      byte[] result = map.get(Arrays.hashCode(qual));
      return result;
    } else
      return null;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    Iterator<Integer> famIter = this.familyMap.keySet().iterator();
    for (int i = 0; famIter.hasNext(); i++) {
      Integer famHash = famIter.next();
      byte[] fam = this.reverseArrayHashMap.get(famHash);
      buf.append(org.cloudgraph.common.Bytes.toString(fam));
      buf.append(" (");
      buf.append(famHash);
      buf.append(")");
      buf.append(": ");
      Map<Integer, byte[]> map = this.familyMap.get(famHash);
      Iterator<Integer> iter = map.keySet().iterator();
      for (int j = 0; iter.hasNext(); j++) {
        buf.append("\n\t");
        Integer qualHash = iter.next();
        byte[] qual = this.reverseArrayHashMap.get(qualHash);
        byte[] value = map.get(qualHash);
        buf.append(org.cloudgraph.common.Bytes.toString(qual));
        buf.append(" (");
        buf.append(qualHash);
        buf.append(")");
        buf.append("\t");
        buf.append(org.cloudgraph.common.Bytes.toString(value));
      }
    }
    return buf.toString();
  }
}
