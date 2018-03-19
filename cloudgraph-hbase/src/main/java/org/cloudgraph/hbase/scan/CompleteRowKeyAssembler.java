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
package org.cloudgraph.hbase.scan;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.key.KeySupport;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.KeyFieldMapping;
//import org.cloudgraph.store.mapping.Padding;
import org.cloudgraph.store.mapping.MetaKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * Assembles a row key where each field within the composite row key is
 * constructed based a set of given query predicates.
 * 
 * @see org.cloudgraph.store.mapping.DataGraphMapping
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.store.mapping.DataField
 * @see org.cloudgraph.store.mapping.MetaField
 * @see org.cloudgraph.store.mapping.MetaFieldName
 * @author Scott Cinnamond
 * @since 0.5.5
 */
public class CompleteRowKeyAssembler implements RowKeyScanAssembler, CompleteRowKey {
  private static Log log = LogFactory.getLog(CompleteRowKeyAssembler.class);
  protected int bufsize = 4000;
  protected ByteBuffer startKey = ByteBuffer.allocate(bufsize);
  protected PlasmaType rootType;
  protected DataGraphMapping graph;
  protected TableMapping table;
  protected KeySupport keySupport = new KeySupport();
  protected Charset charset;
  protected ScanLiterals scanLiterals;
  protected int startRowFieldCount;
  protected String rootUUID;

  // protected Hashing hashing;
  // protected Padding padding;

  @SuppressWarnings("unused")
  private CompleteRowKeyAssembler() {
  }

  /**
   * Constructor
   * 
   * @param rootType
   *          the root type
   */
  public CompleteRowKeyAssembler(PlasmaType rootType) {
    this.rootType = rootType;
    QName rootTypeQname = this.rootType.getQualifiedName();
    this.graph = StoreMapping.getInstance().getDataGraph(rootTypeQname);
    this.table = StoreMapping.getInstance().getTable(rootTypeQname);
    // Hash hash = this.keySupport.getHashAlgorithm(this.table);
    this.charset = StoreMapping.getInstance().getCharset();
    // this.hashing = new Hashing(hash, this.charset);
    // this.padding = new Padding(this.charset);
  }

  /**
   * Constructor which enables the use of data object UUID as a pre-defined row
   * key field. Only applicable for graph predicate "slice" queries.
   * 
   * @param rootType
   *          the root type
   * @param rootUUID
   *          the root UUID.
   */
  public CompleteRowKeyAssembler(PlasmaType rootType, String rootUUID) {
    this(rootType);
    this.rootUUID = rootUUID;
  }

  /**
   * Assemble row key scan information based only on any pre-defined row-key
   * fields such as the data graph root type or URI.
   * 
   * @see org.cloudgraph.store.mapping.MetaField
   * @see org.cloudgraph.store.mapping.MetaFieldName
   */
  @Override
  public void assemble() {
    this.startKey = ByteBuffer.allocate(bufsize);
    assemblePredefinedFields();
  }

  /**
   * Assemble row key scan information based on the given scan literals as well
   * as pre-defined row-key fields such as the data graph root type or URI.
   * 
   * @param literalList
   *          the scan literals
   * @see org.cloudgraph.store.mapping.MetaField
   * @see org.cloudgraph.store.mapping.MetaFieldName
   */
  @Override
  public void assemble(ScanLiterals literals) {
    this.scanLiterals = literals;
    this.startKey = ByteBuffer.allocate(bufsize);
    assembleLiterals();
  }

  /**
   * Assemble row key scan information based on one or more given query
   * predicates.
   * 
   * @param where
   *          the row predicate hierarchy
   * @param contextType
   *          the context type which may be the root type or another type linked
   *          by one or more relations to the root
   */
  @Override
  public void assemble(Where where, PlasmaType contextType) {
    this.startKey = ByteBuffer.allocate(bufsize);

    if (log.isDebugEnabled())
      log.debug("begin traverse");

    ScanLiteralAssembler literalAssembler = new ScanLiteralAssembler(this.rootType);
    where.accept(literalAssembler); // traverse

    this.scanLiterals = literalAssembler.getPartialKeyScanResult();

    if (log.isDebugEnabled())
      log.debug("end traverse");

    assembleLiterals();
  }

  private void assemblePredefinedFields() {
    List<MetaKeyFieldMapping> resultFields = new ArrayList<MetaKeyFieldMapping>();

    for (MetaKeyFieldMapping field : this.graph.getPreDefinedRowKeyFields()) {
      switch (field.getName()) {
      case URI:
      case TYPE:
        resultFields.add(field);
        break;
      default:
      }
    }

    int fieldCount = resultFields.size();
    for (int i = 0; i < fieldCount; i++) {
      MetaKeyFieldMapping preDefinedField = resultFields.get(i);
      if (startRowFieldCount > 0) {
        this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
      }
      Object value = preDefinedField.getKey(this.rootType);
      byte[] encodedValue = preDefinedField.getCodec().encode(value);

      this.startKey.put(encodedValue);
    }
  }

  private void assembleLiterals() {
    for (KeyFieldMapping fieldConfig : this.graph.getRowKeyFields()) {
      if (fieldConfig instanceof MetaKeyFieldMapping) {
        MetaKeyFieldMapping predefinedConfig = (MetaKeyFieldMapping) fieldConfig;

        Object tokenValue = null;
        switch (predefinedConfig.getName()) {
        default:
          tokenValue = predefinedConfig.getKey(this.rootType);
          break;
        }
        byte[] encodedValue = fieldConfig.getCodec().encode(tokenValue);

        if (startRowFieldCount > 0)
          this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
        this.startKey.put(encodedValue);
        this.startRowFieldCount++;
      } else if (fieldConfig instanceof DataRowKeyFieldMapping) {
        DataRowKeyFieldMapping userFieldConfig = (DataRowKeyFieldMapping) fieldConfig;
        List<ScanLiteral> scanLiterals = this.scanLiterals.getLiterals(userFieldConfig);
        if (scanLiterals == null)
          continue;
        for (ScanLiteral scanLiteral : scanLiterals) {
          byte[] startBytes = scanLiteral.getStartBytes();
          if (startBytes.length > 0) {
            if (this.startRowFieldCount > 0) {
              this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
            }
            this.startKey.put(startBytes);
            this.startRowFieldCount++;
          }
        }
      }
    }
  }

  /**
   * Returns the row key as a byte array.
   * 
   * @return the row key
   * @throws IllegalStateException
   *           if the row key is not yet assembled
   */
  @Override
  public byte[] getKey() {
    if (this.startKey == null)
      throw new IllegalStateException("row keys not assembled - first call assemble(...)");
    // ByteBuffer.array() returns unsized array so don't sent that back to
    // clients
    // to misuse.
    // Use native arraycopy() method as it uses native memcopy to create
    // result array
    // and because
    // ByteBuffer.get(byte[] dst,int offset, int length) is not native
    byte[] result = new byte[this.startKey.position()];
    System.arraycopy(this.startKey.array(), this.startKey.arrayOffset(), result, 0,
        this.startKey.position());
    return result;
  }

}
