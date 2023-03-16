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
package org.cloudgraph.rocksdb.scan;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rocksdb.key.KeySupport;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.KeyFieldMapping;
//import org.cloudgraph.store.mapping.Padding;
import org.cloudgraph.store.mapping.MetaKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * Assembles a composite partial row (start/stop) key pair where each field
 * within the composite start and stop row keys are constructed based a set of
 * query predicates.
 * 
 * @see org.cloudgraph.store.mapping.DataGraphMapping
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.store.mapping.DataField
 * @see org.cloudgraph.store.mapping.MetaField
 * @see org.cloudgraph.store.mapping.MetaFieldName
 * @author Scott Cinnamond
 * @since 0.5
 */
public class PartialRowKeyScanAssembler implements RowKeyScanAssembler, PartialRowKey {
  private static Log log = LogFactory.getLog(PartialRowKeyScanAssembler.class);
  protected int bufsize = 4000;
  protected ByteBuffer startKey = ByteBuffer.allocate(bufsize);
  protected ByteBuffer stopKey = ByteBuffer.allocate(bufsize);
  protected PlasmaType rootType;
  protected StoreMappingContext mappingContext;
  protected DataGraphMapping graph;
  protected TableMapping table;
  protected KeySupport keySupport = new KeySupport();
  protected Charset charset;
  protected ScanLiterals scanLiterals;
  protected int startRowFieldCount;
  protected int stopRowFieldCount;
  protected String rootUUID;

  @SuppressWarnings("unused")
  private PartialRowKeyScanAssembler() {
  }

  /**
   * Constructor
   * 
   * @param rootType
   *          the root type
   */
  public PartialRowKeyScanAssembler(PlasmaType rootType, StoreMappingContext mappingContext) {
    this.rootType = rootType;
    this.mappingContext = mappingContext;
    QName rootTypeQname = this.rootType.getQualifiedName();
    this.graph = StoreMapping.getInstance().getDataGraph(rootTypeQname, mappingContext);
    this.table = StoreMapping.getInstance().getTable(rootTypeQname, mappingContext);
    this.charset = StoreMapping.getInstance().getCharset();
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
  public PartialRowKeyScanAssembler(PlasmaType rootType, String rootUUID,
      StoreMappingContext mappingContext) {
    this(rootType, mappingContext);
    this.rootUUID = rootUUID;
  }

  @Override
  public ScanLiterals getScanLiterals() {
    return scanLiterals;
  }

  @Override
  public int getStartRowFieldCount() {
    return startRowFieldCount;
  }

  @Override
  public int getStopRowFieldCount() {
    return stopRowFieldCount;
  }

  /**
   * Assemble row key scan information based only on any meta row-key fields
   * such as the data graph root type or URI.
   * 
   * @see org.cloudgraph.store.mapping.MetaField
   * @see org.cloudgraph.store.mapping.MetaFieldName
   */
  @Override
  public void assemble() {
    this.startKey = ByteBuffer.allocate(bufsize);
    this.stopKey = ByteBuffer.allocate(bufsize);
    assembleMetaFields();
  }

  /**
   * Assemble row key scan information based on the given scan literals as well
   * as meta row-key fields such as the data graph root type or URI.
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
    this.stopKey = ByteBuffer.allocate(bufsize);
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
    this.stopKey = ByteBuffer.allocate(bufsize);

    if (log.isDebugEnabled())
      log.debug("begin traverse");

    ScanLiteralAssembler literalAssembler = new ScanLiteralAssembler(this.rootType,
        this.mappingContext);
    where.accept(literalAssembler); // traverse

    this.scanLiterals = literalAssembler.getPartialKeyScanResult();

    if (log.isDebugEnabled())
      log.debug("end traverse");

    assembleLiterals();
  }

  private void assembleMetaFields() {
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
      MetaKeyFieldMapping metaField = resultFields.get(i);
      if (startRowFieldCount > 0) {
        this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
      }
      if (stopRowFieldCount > 0) {
        this.stopKey.put(graph.getRowKeyFieldDelimiterBytes());
      }

      byte[] paddedStartValue = getStartBytes(metaField);

      this.startKey.put(paddedStartValue);
      startRowFieldCount++;

      byte[] paddedStopValue = getStopBytes(metaField, i >= (fieldCount - 1));
      this.stopKey.put(paddedStopValue);
      stopRowFieldCount++;
    }
  }

  private void assembleLiterals() {
    // first collect the set of field mappings which have literals or
    // meta field mapping value(s), such that we can determine the
    // last field value.
    List<KeyFieldMapping> resultFields = new ArrayList<KeyFieldMapping>();
    for (KeyFieldMapping fieldMapping : this.graph.getRowKeyFields()) {
      if (!(fieldMapping instanceof DataRowKeyFieldMapping)) {
        resultFields.add(fieldMapping);
      } else {
        DataRowKeyFieldMapping dataFieldMapping = (DataRowKeyFieldMapping) fieldMapping;
        List<ScanLiteral> scanLiterals = this.scanLiterals.getLiterals(dataFieldMapping);
        if (scanLiterals != null)
          resultFields.add(fieldMapping);
      }
    }

    int fieldCount = resultFields.size();
    for (int i = 0; i < fieldCount; i++) {
      KeyFieldMapping dataField = resultFields.get(i);
      if (dataField instanceof MetaKeyFieldMapping) {
        MetaKeyFieldMapping predefinedConfig = (MetaKeyFieldMapping) dataField;

        byte[] startBytes = getStartBytes(predefinedConfig);
        byte[] stopBytes = getStopBytes(predefinedConfig, i >= (fieldCount - 1));

        if (startRowFieldCount > 0)
          this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
        if (stopRowFieldCount > 0)
          this.stopKey.put(graph.getRowKeyFieldDelimiterBytes());

        this.startKey.put(startBytes);
        this.stopKey.put(stopBytes);
        this.startRowFieldCount++;
        this.stopRowFieldCount++;
      } else if (dataField instanceof DataRowKeyFieldMapping) {
        DataRowKeyFieldMapping dataFieldMapping = (DataRowKeyFieldMapping) dataField;
        List<ScanLiteral> scanLiterals = this.scanLiterals.getLiterals(dataFieldMapping);
        // We may have multiple literals but all may not have start/stop
        // bytes, e.g. a String literal with a less-than-equal '<=' operator
        // will not have a start bytes.
        for (ScanLiteral scanLiteral : scanLiterals) {
          byte[] startBytes = scanLiteral.getStartBytes();
          if (startBytes.length > 0) {
            if (this.startRowFieldCount > 0) {
              this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
            }
            this.startKey.put(startBytes);
            this.startRowFieldCount++;
          }

          byte[] stopBytes = null;
          // if not last field
          if (i < (fieldCount - 1)
              && scanLiteral.relationalOperator.ordinal() == RelationalOperatorName.EQUALS
                  .ordinal()) {
            stopBytes = scanLiteral.getStartBytes();
          } else {
            // only use stop bytes is last field in (compound) key
            stopBytes = scanLiteral.getStopBytes();
          }

          if (stopBytes.length > 0) {
            if (this.stopRowFieldCount > 0) {
              this.stopKey.put(graph.getRowKeyFieldDelimiterBytes());
            }
            this.stopKey.put(stopBytes);
            this.stopRowFieldCount++;
          }
        }

      }
    }
    // If we have more data fields defined for the key but no
    // more literals. Ordinarily do nothing
    // but with some codecs, we need to terminate the key
    // with the delimiter to ensure e.g. a lexicographic "equals"
    if (resultFields.size() < this.graph.getRowKeyFields().size()) {
      KeyFieldMapping lastField = resultFields.get(resultFields.size() - 1);
      switch (lastField.getCodecType()) {
      case LEXICOSIMPLE:
        this.startKey.put(graph.getRowKeyFieldDelimiterBytes());
        break;
      default:
        break;
      }
    }
  }

  private byte[] getStartBytes(MetaKeyFieldMapping preDefinedField) {
    byte[] encodedStartValue = null;
    switch (preDefinedField.getName()) {
    default:
      Object value = preDefinedField.getKey(this.rootType);
      encodedStartValue = preDefinedField.getCodec().encode(value);
      break;
    }
    return encodedStartValue;
  }

  private byte[] getStopBytes(MetaKeyFieldMapping preDefinedField, boolean lastField) {
    byte[] encodedStopValue = null;
    switch (preDefinedField.getName()) {
    default:
      Object value = preDefinedField.getKey(this.rootType);
      if (!lastField)
        encodedStopValue = preDefinedField.getCodec().encode(value);
      else
        encodedStopValue = preDefinedField.getCodec().encodeNext(value);
      break;
    }
    return encodedStopValue;
  }

  /**
   * Returns the start row key as a byte array.
   * 
   * @return the start row key
   * @throws IllegalStateException
   *           if row keys are not yet assembled
   */
  @Override
  public byte[] getStartKey() {
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

  /**
   * Returns the stop row key as a byte array.
   * 
   * @return the stop row key
   * @throws IllegalStateException
   *           if row keys are not yet assembled
   */
  @Override
  public byte[] getStopKey() {
    if (this.stopKey == null)
      throw new IllegalStateException("row keys not assembled - first call assemble(...)");
    byte[] result = new byte[this.stopKey.position()];
    System.arraycopy(this.stopKey.array(), this.stopKey.arrayOffset(), result, 0,
        this.stopKey.position());
    return result;
  }
}
