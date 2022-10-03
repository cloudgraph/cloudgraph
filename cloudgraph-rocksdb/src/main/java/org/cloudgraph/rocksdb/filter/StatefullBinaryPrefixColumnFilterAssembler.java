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
package org.cloudgraph.rocksdb.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.filter.BinaryComparator;
//import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
//import org.apache.hadoop.hbase.filter.CompareFilter;
//import org.apache.hadoop.hbase.filter.FilterList;
//import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.cloudgraph.rocksdb.io.EdgeReader;
import org.cloudgraph.rocksdb.key.StatefullColumnKeyFactory;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

/**
 * Creates an column filter.
 * 
 * @see org.cloudgraph.store.key.GraphStatefullColumnKeyFactory
 * @see org.cloudgraph.rocksdb.key.StatefullColumnKeyFactory
 * @author Scott Cinnamond
 * @since 0.5
 */
public class StatefullBinaryPrefixColumnFilterAssembler extends FilterListAssembler {
  private static Log log = LogFactory.getLog(StatefullBinaryPrefixColumnFilterAssembler.class);
  private StatefullColumnKeyFactory columnKeyFac;
  private EdgeReader edgeReader;
  private byte[] family;

  public StatefullBinaryPrefixColumnFilterAssembler(PlasmaType rootType, EdgeReader edgeReader) {
    super(rootType);
    this.edgeReader = edgeReader;
    // this.rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    this.columnKeyFac = new StatefullColumnKeyFactory(rootType, this.edgeReader.getRowReader()
        .getMappingContext());
    this.family = this.columnKeyFac.getTable().getDataColumnFamilyNameBytes();
  }

  public void assemble(Set<Property> properies, Set<Long> sequences, PlasmaType contextType) {
    if (sequences == null || sequences.size() == 0)
      throw new IllegalArgumentException("expected one or more sequences");
    byte[] colKey = null;
    // QualifierFilter qualFilter = null;
    PlasmaType subType = edgeReader.getSubType();
    if (subType == null)
      subType = edgeReader.getBaseType();

    for (Long seq : sequences) {
      for (EntityMetaKey metaField : EntityMetaKey.values()) {
        colKey = this.columnKeyFac.createColumnKey(subType, seq, metaField);
        ColumnInfo ci = new ColumnInfo(this.family, colKey, metaField.getStorageType());
        this.prefixMap.put(ci.getColumn(), ci);
        if (log.isDebugEnabled())
          log.debug("collected " + ci);
      }

      for (Property p : properies) {
        PlasmaProperty prop = (PlasmaProperty) p;
        if (prop.getType().isDataType()) {
          colKey = this.columnKeyFac.createColumnKey(subType, seq, prop);
          ColumnInfo ci = new ColumnInfo(this.family, colKey, subType, prop);
          this.prefixMap.put(ci.getColumn(), ci);
          if (log.isDebugEnabled())
            log.debug("collected " + ci);

        } else {
          colKey = this.columnKeyFac.createColumnKey(subType, seq, prop);
          for (EdgeMetaKey metaField : EdgeMetaKey.values()) {
            colKey = this.columnKeyFac.createColumnKey(subType, prop, metaField);
            ColumnInfo ci = new ColumnInfo(this.family, colKey, subType, prop);
            this.prefixMap.put(ci.getColumn(), ci);
            if (log.isDebugEnabled())
              log.debug("collected " + ci);
          }
        }
      }
    }
  }
}
