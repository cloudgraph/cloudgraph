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
import org.apache.hadoop.hbase.util.Hash;
import org.cloudgraph.hbase.key.Hashing;
import org.cloudgraph.hbase.key.KeySupport;
import org.cloudgraph.hbase.key.Padding;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.cloudgraph.store.mapping.PreDefinedKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.mapping.UserDefinedRowKeyFieldMapping;
import org.plasma.query.model.Where;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaType;

/**
 * Assembles a row key where each field within the composite row key is
 * constructed based a set of given query predicates.
 * 
 * @see org.cloudgraph.store.mapping.DataGraphMapping
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.store.mapping.UserDefinedField
 * @see org.cloudgraph.store.mapping.PredefinedField
 * @see org.cloudgraph.store.mapping.PreDefinedFieldName
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
  protected Hashing hashing;
  protected Padding padding;

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
    Hash hash = this.keySupport.getHashAlgorithm(this.table);
    this.charset = StoreMapping.getInstance().getCharset();
    this.hashing = new Hashing(hash, this.charset);
    this.padding = new Padding(this.charset);
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
   * @see org.cloudgraph.store.mapping.PredefinedField
   * @see org.cloudgraph.store.mapping.PreDefinedFieldName
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
   * @see org.cloudgraph.store.mapping.PredefinedField
   * @see org.cloudgraph.store.mapping.PreDefinedFieldName
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
    List<PreDefinedKeyFieldMapping> resultFields = new ArrayList<PreDefinedKeyFieldMapping>();

    for (PreDefinedKeyFieldMapping field : this.graph.getPreDefinedRowKeyFields()) {
      switch (field.getName()) {
      case URI:
      case TYPE:
        resultFields.add(field);
        break;
      case UUID:
        break; // not applicable
      default:
      }
    }

    int fieldCount = resultFields.size();
    for (int i = 0; i < fieldCount; i++) {
      PreDefinedKeyFieldMapping preDefinedField = resultFields.get(i);
      if (startRowFieldCount > 0) {
        this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
      }

      byte[] startValue = this.keySupport.getPredefinedFieldValueStartBytes(this.rootType, hashing,
          preDefinedField);
      byte[] paddedStartValue = null;
      if (preDefinedField.isHash()) {
        paddedStartValue = this.padding.pad(startValue, preDefinedField.getMaxLength(),
            DataFlavor.integral);
      } else {
        paddedStartValue = this.padding.pad(startValue, preDefinedField.getMaxLength(),
            preDefinedField.getDataFlavor());
      }

      this.startKey.put(paddedStartValue);
    }
  }

  private void assembleLiterals() {
    for (KeyFieldMapping fieldConfig : this.graph.getRowKeyFields()) {
      if (fieldConfig instanceof PreDefinedKeyFieldMapping) {
        PreDefinedKeyFieldMapping predefinedConfig = (PreDefinedKeyFieldMapping) fieldConfig;

        byte[] tokenValue = null;
        switch (predefinedConfig.getName()) {
        case UUID:
          if (this.rootUUID != null) {
            tokenValue = this.rootUUID.getBytes(this.charset);
            break;
          } else
            continue;
        default:
          tokenValue = predefinedConfig.getKeyBytes(this.rootType);
          break;
        }
        // FIXME: if predefined field is last, need stop bytes

        byte[] paddedTokenValue = null;
        if (fieldConfig.isHash()) {
          tokenValue = hashing.toStringBytes(tokenValue);
          paddedTokenValue = this.padding.pad(tokenValue, predefinedConfig.getMaxLength(),
              DataFlavor.integral);
        } else {
          paddedTokenValue = this.padding.pad(tokenValue, predefinedConfig.getMaxLength(),
              predefinedConfig.getDataFlavor());
        }

        if (startRowFieldCount > 0)
          this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
        this.startKey.put(paddedTokenValue);
        this.startRowFieldCount++;
      } else if (fieldConfig instanceof UserDefinedRowKeyFieldMapping) {
        UserDefinedRowKeyFieldMapping userFieldConfig = (UserDefinedRowKeyFieldMapping) fieldConfig;
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
