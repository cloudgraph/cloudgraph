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

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
//import org.apache.hadoop.hbase.filter.CompareFilter;
//import org.apache.hadoop.hbase.filter.FilterList;
//import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.cloudgraph.rocksdb.key.CompositeColumnKeyFactory;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

/**
 * Creates an HBase column filter list using <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/QualifierFilter.html"
 * >QualifierFilter</a> and <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/BinaryPrefixComparator.html"
 * >BinaryPrefixComparator</a> and recreating composite column qualifier
 * prefixes for comparison using {@link CompositeColumnKeyFactory}.
 * <p>
 * HBase filters may be collected into lists using <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html"
 * target="#">FilterList</a> each with a <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ALL"
 * target="#">MUST_PASS_ALL</a> or <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ONE"
 * target="#">MUST_PASS_ONE</a> (logical) operator. Lists may then be assembled
 * into hierarchies used to represent complex expression trees filtering either
 * rows or columns in HBase.
 * </p>
 * 
 * @see org.cloudgraph.store.key.GraphColumnKeyFactory
 * @see org.cloudgraph.rocksdb.key.CompositeColumnKeyFactory
 * @author Scott Cinnamond
 * @since 0.5
 */
public class BinaryPrefixColumnFilterAssembler extends FilterListAssembler {
  private static Log log = LogFactory.getLog(BinaryPrefixColumnFilterAssembler.class);
  private GraphColumnKeyFactory columnKeyFac;

  public BinaryPrefixColumnFilterAssembler(PlasmaType rootType, StoreMappingContext mappingContext) {
    super(rootType);
    this.columnKeyFac = new CompositeColumnKeyFactory(rootType, mappingContext);

    // this.rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
  }

  public void assemble(Set<Property> properties, PlasmaType contextType) {
    // Note: using many binary prefix qualifier filters
    // rather than a single MultipleColumnPrefixFilter under the
    // assumption that the binary compare is more
    // efficient than the string conversion
    // required by the MultipleColumnPrefixFilter (?)
    // for (Property p : properties) {
    // PlasmaProperty prop = (PlasmaProperty) p;
    // byte[] key = this.columnKeyFac.createColumnKey(contextType, prop);
    // QualifierFilter qualFilter = new
    // QualifierFilter(CompareFilter.CompareOp.EQUAL,
    // new BinaryPrefixComparator(key));
    // this.rootFilter.addFilter(qualFilter);
    // }
  }
}
