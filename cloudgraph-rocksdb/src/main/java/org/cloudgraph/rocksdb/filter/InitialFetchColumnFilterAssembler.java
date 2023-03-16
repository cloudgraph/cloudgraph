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

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.filter.CompareFilter;
//import org.apache.hadoop.hbase.filter.FilterList;
//import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
//import org.apache.hadoop.hbase.filter.QualifierFilter;
//import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.rocksdb.RocksDBConstants;
import org.cloudgraph.rocksdb.key.CompositeColumnKeyFactory;
import org.cloudgraph.rocksdb.key.StatefullColumnKeyFactory;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.collector.Selection;
import org.plasma.query.model.Select;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.xml.sax.SAXException;

import commonj.sdo.Property;
import commonj.sdo.Type;

/**
 * Creates an HBase column filter set based on the given criteria which
 * leverages the HBase <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/MultipleColumnPrefixFilter.html"
 * >MultipleColumnPrefixFilter</a> to return only columns for the graph root as
 * well as any columns associated with type nodes linked through one of more
 * singular relations.
 * 
 * @see GraphColumnKeyFactory
 * @see CompositeColumnKeyFactory
 * @see GraphFetchColumnFilterAssembler
 * @author Scott Cinnamond
 * @since 0.5
 */
public class InitialFetchColumnFilterAssembler extends FilterListAssembler implements
    RocksDBConstants {
  private static Log log = LogFactory.getLog(InitialFetchColumnFilterAssembler.class);

  private StatefullColumnKeyFactory columnKeyFac;
  private Map<String, ColumnInfo> prefixMap = new HashMap<String, ColumnInfo>();
  private Selection selection;
  private Charset charset;
  private byte[] family;

  public InitialFetchColumnFilterAssembler(Selection collector, PlasmaType rootType,
      StoreMappingContext mappingContext) {
    super(rootType);
    this.selection = collector;
    this.columnKeyFac = new StatefullColumnKeyFactory(rootType, mappingContext);
    this.charset = StoreMapping.getInstance().getCharset();
    this.family = this.columnKeyFac.getTable().getDataColumnFamilyNameBytes();

    // add default filters for graph state info needed for all queries
    for (GraphMetaKey field : GraphMetaKey.values()) {
      ColumnInfo ci = new ColumnInfo(this.family, field.codeAsBytes(), field.getStorageType());
      this.prefixMap.put(ci.getColumn(), ci);
    }

    collect();
  }

  @Override
  public Filter getFilter() {
    return new MultiColumnPrefixFilter(prefixMap);
  }

  /**
   * Collects and maps column prefixes used to create HBase filter(s) for column
   * selection.
   * 
   * @param select
   *          the select clause
   */
  private void collect() {
    byte[] colKey = null;
    String colKeyStr;
    for (Type t : this.selection.getInheritedTypes()) {
      PlasmaType type = (PlasmaType) t;
      if (!rootType.equals(type))
        continue; // interested in only root for "initial" fetch

      // adds entity level meta data qualifier prefixes root type
      for (EntityMetaKey metaField : EntityMetaKey.values()) {
        colKey = this.columnKeyFac.createColumnKey(type, metaField);
        ColumnInfo ci = new ColumnInfo(this.family, colKey, metaField.getStorageType());
        this.prefixMap.put(ci.getColumn(), ci);
        if (log.isDebugEnabled())
          log.debug("collected: " + ci);
      }

      Set<Property> props = this.selection.getInheritedProperties(type);
      for (Property p : props) {
        PlasmaProperty prop = (PlasmaProperty) p;
        colKey = this.columnKeyFac.createColumnKey(type, prop);
        ColumnInfo ci = new ColumnInfo(this.family, colKey, type, prop);
        this.prefixMap.put(ci.getColumn(), ci);
        if (log.isDebugEnabled())
          log.debug("collected " + ci);
      }
    }
  }

  protected void log(Select root) {
    String xml = "";
    PlasmaQueryDataBinding binding;
    try {
      binding = new PlasmaQueryDataBinding(new DefaultValidationEventHandler());
      xml = binding.marshal(root);
    } catch (JAXBException e) {
      log.debug(e);
    } catch (SAXException e) {
      log.debug(e);
    }
    log.debug("query: " + xml);
  }

}
