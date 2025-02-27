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
package org.cloudgraph.core.key;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import javax.xml.namespace.QName;

import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.io.RowOperation;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.MappingConfiguration;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.sdo.PlasmaType;

/**
 * A configuration driven abstract class which helps subclasses leverage
 * {@link org.cloudgraph.store.mapping.TableMapping table} and
 * {@link org.cloudgraph.store.mapping.DataGraphMapping data graph} specific
 * configuration information, such as the hashing algorithm and field level row
 * and column model settings, as well as java <a target="#" href=
 * "http://docs.oracle.com/javase/1.5.0/docs/api/java/nio/ByteBuffer.html"
 * >ByteBuffer</a> for composite row and column key creation using byte arrays.
 * <p>
 * The initial creation and subsequent reconstitution for query retrieval
 * purposes of both row and column keys in CloudGraph&#8482; is efficient, as it
 * leverages byte array level API in both Java and the current underlying SDO
 * 2.1 implementation, <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a>. Both composite row and
 * column keys are composed in part of structural metadata, and the lightweight
 * metadata API within <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a> contains byte-array level,
 * cached lookup of all basic metadata elements including logical and physical
 * type and property names.
 * </p>
 * 
 * @see org.cloudgraph.store.mapping.StoreMapping
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.store.mapping.DataGraphMapping
 * @author Scott Cinnamond
 * @since 0.5
 */
public abstract class ByteBufferKeyFactory implements ConfigurableKeyFactory {
  protected int bufsize = 4000;
  protected ByteBuffer buf = ByteBuffer.allocate(bufsize);

  protected Charset charset;
  protected KeySupport keySupport = new KeySupport();
  protected TableMapping table;
  protected DataGraphMapping graph;
  protected PlasmaType rootType;

  @SuppressWarnings("unused")
  private ByteBufferKeyFactory() {
  }

  /**
   * Constructor for read/write operations where we have already found or are
   * creating the underlying row. Not for query operations where we have nothing
   * but metadata.
   * 
   * @param rowOperation
   */
  protected ByteBufferKeyFactory(RowOperation rowOperation) {
    this.table = rowOperation.getDataGraph().getTable();
    this.charset = table.getCharset();
    this.graph = rowOperation.getDataGraph();
  }

  /**
   * Constructor given pure metadata without any operational state, which looks
   * up table and data graph specific configuration information for the given
   * SDO type.
   * 
   * @param rootType
   *          the SDO type
   */
  protected ByteBufferKeyFactory(PlasmaType rootType, StoreMappingContext mappingContext) {
    this.rootType = rootType;
    // FIXME: should be table context delegate?
    QName rootTypeQname = this.rootType.getQualifiedName();
    MappingConfiguration config = StoreMapping.getInstance();
    if (config.findTable(rootTypeQname, mappingContext) == null)
      throw new IllegalArgumentException("given type is not a bound (graph root) type, " + rootType);
    this.table = config.getTable(rootTypeQname, mappingContext);
    this.graph = config.getDataGraph(rootTypeQname, mappingContext);
    this.charset = config.getCharset();
  }

  public TableMapping getTable() {
    return this.table;
  }

  public DataGraphMapping getGraph() {
    return this.graph;
  }

  public ByteBuffer getBuf() {
    return buf;
  }

  public PlasmaType getRootType() {
    return this.rootType;
  }

}
