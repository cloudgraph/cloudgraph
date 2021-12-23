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
package org.cloudgraph.aerospike.filter;

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
import org.cloudgraph.aerospike.key.CompositeColumnKeyFactory;
import org.cloudgraph.aerospike.key.StatefullColumnKeyFactory;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.collector.Selection;
import org.plasma.query.model.Select;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.xml.sax.SAXException;

import commonj.sdo.Property;
import commonj.sdo.Type;

/**
 * Creates an HBase column filter set based on the given criteria which
 * leverages the HBase <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/MultipleColumnPrefixFilter.html"
 * >MultipleColumnPrefixFilter</a> to return only columns which make up the
 * selected data graph. The advantage of this strategy is that a complete graph
 * of any complexity may be returned in a single round trip.
 * 
 * @see GraphColumnKeyFactory
 * @see InitialFetchColumnFilterAssembler
 * @author Scott Cinnamond
 * @since 0.5
 */
public class GraphFetchColumnFilterAssembler extends FilterListAssembler implements
    AerospikeFilterAssembler {
  private static Log log = LogFactory.getLog(GraphFetchColumnFilterAssembler.class);

  private StatefullColumnKeyFactory columnKeyFac;
  private Map<String, ColumnInfo> prefixMap = new HashMap<String, ColumnInfo>();
  private Selection propertySelection;

  public GraphFetchColumnFilterAssembler(Selection selection, PlasmaType rootType,
      StoreMappingContext mappingContext) {

    super(rootType);
    this.propertySelection = selection;
    this.columnKeyFac = new StatefullColumnKeyFactory(rootType, mappingContext);

    // this.rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    //
    // // add default filters for graph state info needed for all queries
    // QualifierFilter filter = null;
    // for (GraphMetaKey field : GraphMetaKey.values()) {
    // filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new
    // SubstringComparator(
    // field.code()));
    // this.rootFilter.addFilter(filter);
    // }

    collect();

    // byte[][] prefixes = new byte[this.prefixMap.size()][];
    // int i = 0;
    // for (byte[] prefix : this.prefixMap.values()) {
    // prefixes[i] = prefix;
    // i++;
    // }
    // MultipleColumnPrefixFilter multiPrefixfilter = new
    // MultipleColumnPrefixFilter(prefixes);
    //
    // this.rootFilter.addFilter(multiPrefixfilter);

  }

  @Override
  public Filter getFilter() {
    return new Filter(prefixMap);
  }

  public void clear() {
    super.clear();
    this.prefixMap.clear();
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
    for (Type type : this.propertySelection.getInheritedTypes()) {
      PlasmaType plasmaType = (PlasmaType) type;
      // adds entity level meta data qualifier prefixes for ALL types
      // in the selection
      for (EntityMetaKey metaField : EntityMetaKey.values()) {
        colKey = this.columnKeyFac.createColumnKey(plasmaType, metaField);
        colKeyStr = Bytes.toString(colKey);
        if (colKeyStr.length() > 14)
          throw new IllegalStateException("key/bin overflow: '" + colKeyStr + "'");
        ColumnInfo ci = new ColumnInfo(colKeyStr, colKey, plasmaType);
        this.prefixMap.put(colKeyStr, ci);

        for (int i = 1; i < 10; i++) {
          byte[] edgecolKey = this.columnKeyFac.createColumnKey(plasmaType, i, metaField);
          String edgeColumnKeyStr = Bytes.toString(edgecolKey);
          if (edgeColumnKeyStr.length() > 14)
            throw new IllegalStateException("key/bin overflow: '" + edgeColumnKeyStr + "'");
          ci = new ColumnInfo(edgeColumnKeyStr, edgecolKey, plasmaType);
          this.prefixMap.put(edgeColumnKeyStr, ci);
        }
        if (log.isDebugEnabled())
          log.debug("collected " + ci);
      }

      Set<Property> props = this.propertySelection.getInheritedProperties(type);
      for (Property prop : props) {
        PlasmaProperty plasmaProperty = (PlasmaProperty) prop;
        colKey = this.columnKeyFac.createColumnKey(plasmaType, (PlasmaProperty) prop);
        colKeyStr = Bytes.toString(colKey);
        if (colKeyStr.length() > 14)
          throw new IllegalStateException("key/bin overflow: '" + colKeyStr + "'");
        ColumnInfo ci = new ColumnInfo(colKeyStr, colKey, plasmaType);
        ci.setProperty(plasmaProperty);
        this.prefixMap.put(colKeyStr, ci);
        if (log.isDebugEnabled())
          log.debug("collected " + ci);
        for (int i = 1; i < 10; i++) {
          byte[] edgecolKey = this.columnKeyFac.createColumnKey(plasmaType, i,
              (PlasmaProperty) prop);
          String edgeColumnKeyStr = Bytes.toString(edgecolKey);
          if (edgeColumnKeyStr.length() > 14)
            throw new IllegalStateException("key/bin overflow: '" + edgeColumnKeyStr + "'");
          ci = new ColumnInfo(edgeColumnKeyStr, edgecolKey, plasmaType);
          ci.setProperty(plasmaProperty);
          this.prefixMap.put(edgeColumnKeyStr, ci);
          if (log.isDebugEnabled())
            log.debug("collected " + ci);
        }

        if (!plasmaProperty.getType().isDataType()) {
          for (EdgeMetaKey metaField : EdgeMetaKey.values()) {
            colKey = this.columnKeyFac.createColumnKey(plasmaType, plasmaProperty, metaField);
            colKeyStr = Bytes.toString(colKey);
            ci = new ColumnInfo(colKeyStr, colKey, plasmaType);
            ci.setProperty(plasmaProperty);
            this.prefixMap.put(colKeyStr, ci);
            if (log.isDebugEnabled())
              log.debug("collected " + ci);
            for (int i = 1; i < 10; i++) {
              byte[] edgecolKey = this.columnKeyFac.createColumnKey(plasmaType, i,
                  (PlasmaProperty) prop, metaField);
              String edgeColumnKeyStr = Bytes.toString(edgecolKey);
              ci = new ColumnInfo(edgeColumnKeyStr, edgecolKey, plasmaType);
              ci.setProperty(plasmaProperty);
              this.prefixMap.put(edgeColumnKeyStr, ci);
              if (log.isDebugEnabled())
                log.debug("collected " + ci);
            }
          }
        }
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
