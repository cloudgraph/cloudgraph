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
package org.cloudgraph.hbase.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.key.CompositeColumnKeyFactory;
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
public class InitialFetchColumnFilterAssembler extends FilterListAssembler {
  private static Log log = LogFactory.getLog(InitialFetchColumnFilterAssembler.class);

  private GraphColumnKeyFactory columnKeyFac;
  private Map<String, byte[]> prefixMap = new HashMap<String, byte[]>();
  private Selection selection;

  public InitialFetchColumnFilterAssembler(Selection collector, PlasmaType rootType,
      StoreMappingContext mappingContext) {
    super(rootType);
    this.selection = collector;
    this.columnKeyFac = new CompositeColumnKeyFactory(rootType, mappingContext);

    this.rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    // add default filters for graph state info needed for all queries
    QualifierFilter filter = null;
    for (GraphMetaKey field : GraphMetaKey.values()) {
      filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(
          field.code()));
      this.rootFilter.addFilter(filter);
    }

    collect();

    byte[][] prefixes = new byte[prefixMap.size()][];
    int i = 0;
    for (byte[] prefix : prefixMap.values()) {
      prefixes[i] = prefix;
      i++;
    }

    MultipleColumnPrefixFilter multiPrefixfilter = new MultipleColumnPrefixFilter(prefixes);

    this.rootFilter.addFilter(multiPrefixfilter);
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

      // adds entity level meta data qualifier prefixes for ALL types
      // in the selection
      for (EntityMetaKey metaField : EntityMetaKey.values()) {
        colKey = this.columnKeyFac.createColumnKey(type, metaField);
        colKeyStr = Bytes.toString(colKey);
        this.prefixMap.put(colKeyStr, colKey);
      }

      Set<Property> props = this.selection.getInheritedProperties(type);
      for (Property p : props) {
        PlasmaProperty prop = (PlasmaProperty) p;
        colKey = this.columnKeyFac.createColumnKey(type, prop);
        colKeyStr = Bytes.toString(colKey);
        this.prefixMap.put(colKeyStr, colKey);

        if (log.isDebugEnabled())
          log.debug("collected " + prop);
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
