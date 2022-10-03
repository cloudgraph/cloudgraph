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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.rocksdb.RocksDBConstants;
import org.cloudgraph.rocksdb.key.CompositeColumnKeyFactory;
import org.cloudgraph.rocksdb.key.StatefullColumnKeyFactory;
import org.cloudgraph.rocksdb.service.RocksDBDataConverter;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.lang.GraphFilterException;
import org.cloudgraph.store.lang.InvalidOperatorException;
import org.cloudgraph.store.mapping.StoreMapping;
import org.plasma.query.model.AbstractPathElement;
import org.plasma.query.model.Expression;
import org.plasma.query.model.Literal;
import org.plasma.query.model.LogicalOperator;
import org.plasma.query.model.NullLiteral;
import org.plasma.query.model.Path;
import org.plasma.query.model.PathElement;
import org.plasma.query.model.PredicateOperator;
import org.plasma.query.model.Property;
import org.plasma.query.model.QueryConstants;
import org.plasma.query.model.Term;
import org.plasma.query.model.WildcardPathElement;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.helper.DataConverter;

/**
 * Creates an Aerospike value and qualifier filter hierarchy.
 * 
 * @author Scott Cinnamond
 * @since 2.0.0
 */
public class ColumnPredicateVisitor extends PredicateVisitor implements RocksDBConstants {
  private static Log log = LogFactory.getLog(ColumnPredicateVisitor.class);
  protected StatefullColumnKeyFactory columnKeyFac;
  protected String contextPropertyPath;
  protected Property contextQueryProperty;
  protected PredicateUtil predicateUtil = new PredicateUtil();
  private Charset charset;

  public ColumnPredicateVisitor(PlasmaType rootType) {
    super(rootType);
    this.charset = StoreMapping.getInstance().getCharset();
  }

  /**
   * Process the traversal start event for a query
   * {@link org.plasma.query.model.Property property} within an
   * {@link org.plasma.query.model.Expression expression} just traversing the
   * property path if exists and capturing context information for the current
   * {@link org.plasma.query.model.Expression expression}.
   * 
   * @see org.plasma.query.visitor.DefaultQueryVisitor#start(org.plasma.query.model.Property)
   */
  @Override
  public void start(Property property) {
    Path path = property.getPath();
    PlasmaType targetType = (PlasmaType) this.contextType;
    if (path != null) {
      for (int i = 0; i < path.getPathNodes().size(); i++) {
        AbstractPathElement pathElem = path.getPathNodes().get(i).getPathElement();
        if (pathElem instanceof WildcardPathElement)
          throw new DataAccessException(
              "wildcard path elements applicable for 'Select' clause paths only, not 'Where' clause paths");
        PathElement namedPathElem = ((PathElement) pathElem);
        PlasmaProperty prop = (PlasmaProperty) targetType.getProperty(namedPathElem.getValue());
        namedPathElem.setPhysicalNameBytes(prop.getPhysicalNameBytes());
        targetType = (PlasmaType) prop.getType(); // traverse
      }
    }
    PlasmaProperty endpointProp = (PlasmaProperty) targetType.getProperty(property.getName());
    this.contextProperty = endpointProp;
    this.contextType = targetType;
    this.contextPropertyPath = property.asPathString();
    this.contextQueryProperty = property;
    byte[] colKey = this.columnKeyFac.createColumnKey(this.contextType, this.contextProperty);
    byte[] family = this.columnKeyFac.getTable().getDataColumnFamilyNameBytes();
    this.contextQueryProperty.setPhysicalNameBytes(colKey);

    ColumnInfo ci = new ColumnInfo(family, colKey, this.contextType, this.contextProperty);
    this.columnMap.put(ci.getColumn(), ci);
    if (log.isDebugEnabled())
      log.debug("collected " + ci);

    // adds entity level meta data qualifier prefixes root type
    if (!this.contextProperty.getType().isDataType()) {
      for (EntityMetaKey metaField : EntityMetaKey.values()) {
        colKey = this.columnKeyFac.createColumnKey(this.contextType, metaField);
        ci = new ColumnInfo(family, colKey, metaField.getStorageType());
        this.columnMap.put(ci.getColumn(), ci);
        if (log.isDebugEnabled())
          log.debug("collected: " + ci);
      }
    }

    super.start(property);
  }

}
