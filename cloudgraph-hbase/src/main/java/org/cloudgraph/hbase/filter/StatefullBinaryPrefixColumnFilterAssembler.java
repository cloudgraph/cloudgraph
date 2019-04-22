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

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.key.StatefullColumnKeyFactory;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

/**
 * Creates an HBase column filter list using <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/QualifierFilter.html"
 * >QualifierFilter</a> and <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/BinaryPrefixComparator.html"
 * >BinaryPrefixComparator</a> and recreating composite column qualifier
 * prefixes for comparison using {@link StatefullColumnKeyFactory}.
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
 * @see org.cloudgraph.store.key.GraphStatefullColumnKeyFactory
 * @see org.cloudgraph.hbase.key.StatefullColumnKeyFactory
 * @author Scott Cinnamond
 * @since 0.5
 */
public class StatefullBinaryPrefixColumnFilterAssembler extends FilterListAssembler {
  private static Log log = LogFactory.getLog(StatefullBinaryPrefixColumnFilterAssembler.class);
  private GraphStatefullColumnKeyFactory columnKeyFac;
  private EdgeReader edgeReader;

  public StatefullBinaryPrefixColumnFilterAssembler(PlasmaType rootType, EdgeReader edgeReader) {
    super(rootType);
    this.edgeReader = edgeReader;
    this.rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    this.columnKeyFac = new StatefullColumnKeyFactory(rootType, this.edgeReader.getRowReader()
        .getMappingContext());
  }

  public void assemble(Set<Property> properies, Set<Long> sequences, PlasmaType contextType) {
    if (sequences == null || sequences.size() == 0)
      throw new IllegalArgumentException("expected one or more sequences");
    byte[] colKey = null;
    QualifierFilter qualFilter = null;
    PlasmaType subType = edgeReader.getSubType();
    if (subType == null)
      subType = edgeReader.getBaseType();

    for (Long seq : sequences) {
      // Adds entity level meta data qualifier prefixes for ALL sequences
      // in the selection. Entity meta keys are exact qualifier filters, not
      // prefixes
      for (EntityMetaKey metaField : EntityMetaKey.values()) {
        colKey = this.columnKeyFac.createColumnKey(subType, seq, metaField);
        qualFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(colKey));
        this.rootFilter.addFilter(qualFilter);
      }

      for (Property p : properies) {
        PlasmaProperty prop = (PlasmaProperty) p;
        if (prop.getType().isDataType()) {
          colKey = this.columnKeyFac.createColumnKey(subType, seq, prop);
          qualFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
              colKey));
          this.rootFilter.addFilter(qualFilter);

        } else {
          // reference props have several meta keys so are
          // only property type requiring a prefix filter.
          colKey = this.columnKeyFac.createColumnKey(subType, seq, prop);
          qualFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(colKey));
          this.rootFilter.addFilter(qualFilter);
        }
      }
    }
  }
}
