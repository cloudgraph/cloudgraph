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
package org.cloudgraph.core.scan;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.cloudgraph.store.mapping.MetaKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.query.Wildcard;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * Assembles a composite fuzzy row key bytes / fuzzy info bytes pair where each
 * field within the composite row keys are constructed based a set of query
 * predicates.
 * 
 * @see org.cloudgraph.store.mapping.DataGraphMapping
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.store.mapping.DataField
 * @see org.cloudgraph.store.mapping.MetaField
 * @see org.cloudgraph.store.mapping.MetaFieldName
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class FuzzyRowKeyAssembler implements RowKeyAssembler, FuzzyRowKey {
  private static Log log = LogFactory.getLog(FuzzyRowKeyAssembler.class);
  protected int bufsize = 4000;
  protected ByteBuffer keyBytes = ByteBuffer.allocate(bufsize);
  protected ByteBuffer infoBytes = ByteBuffer.allocate(bufsize);
  protected PlasmaType rootType;
  protected ServiceContext serviceContext;
  protected StoreMappingContext mappingContext;
  protected DataGraphMapping graph;
  protected TableMapping table;
  protected ScanLiterals scanLiterals;
  protected int fieldCount;
  protected String rootUUID;
  protected byte fixedMaskByte = 0;
  protected byte variableMaskByte = 1;
  protected byte[] delimMask;

  @SuppressWarnings("unused")
  private FuzzyRowKeyAssembler() {
  }

  /**
   * Constructor
   * 
   * @param rootType
   *          the root type
   */
  public FuzzyRowKeyAssembler(PlasmaType rootType, ServiceContext serviceContext) {
    this.rootType = rootType;
    this.serviceContext = serviceContext;
    this.mappingContext = serviceContext.getStoreMapping();
    QName rootTypeQname = this.rootType.getQualifiedName();
    this.graph = StoreMapping.getInstance().getDataGraph(rootTypeQname, this.mappingContext);
    this.table = StoreMapping.getInstance().getTable(rootTypeQname, this.mappingContext);
    this.delimMask = new byte[graph.getRowKeyFieldDelimiterBytes().length];
    for (int i = 0; i < delimMask.length; i++)
      delimMask[i] = fixedMaskByte;
  }

  /**
   * Constructor which enables the use of data object UUID as a pre-defined row
   * key field.
   * 
   * @param rootType
   *          the root type
   * @param rootUUID
   *          the root UUID.
   */
  public FuzzyRowKeyAssembler(PlasmaType rootType, String rootUUID, ServiceContext serviceContext) {
    this(rootType, serviceContext);
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
    assembleMetaFields();
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

    if (log.isDebugEnabled())
      log.debug("begin traverse");

    ScanLiteralAssembler literalAssembler = new ScanLiteralAssembler(this.rootType,
        this.serviceContext);
    where.accept(literalAssembler); // traverse

    this.scanLiterals = literalAssembler.getPartialKeyScanResult();

    if (log.isDebugEnabled())
      log.debug("end traverse");

    assembleLiterals();
  }

  private void assembleMetaFields() {
    List<MetaKeyFieldMapping> metaFields = this.graph.getPreDefinedRowKeyFields();
    int fieldCount = metaFields.size();
    for (int i = 0; i < fieldCount; i++) {
      MetaKeyFieldMapping metaField = metaFields.get(i);
      if (fieldCount > 0) {
        this.keyBytes.put(graph.getRowKeyFieldDelimiterBytes());
        this.infoBytes.put(delimMask);
      }
      Object keyValue = metaField.getKey(this.rootType);

      byte[] encodedKeyValue = metaField.getCodec().encode(keyValue);

      this.keyBytes.put(encodedKeyValue);
      byte[] tokenMask = new byte[encodedKeyValue.length];
      for (int j = 0; j < tokenMask.length; j++)
        tokenMask[j] = fixedMaskByte;
      this.infoBytes.put(tokenMask);

      fieldCount++;
    }
  }

  private void assembleLiterals() {
    for (KeyFieldMapping fieldMapping : this.graph.getRowKeyFields()) {
      if (fieldCount > 0) {
        this.keyBytes.put(graph.getRowKeyFieldDelimiterBytes());
        this.infoBytes.put(delimMask);
      }

      if (!(fieldMapping instanceof DataRowKeyFieldMapping)) {
        byte[] encodedValue = getEncodedKeyField(fieldMapping);
        this.keyBytes.put(encodedValue);
        byte[] tokenMask = getKeyFieldMask(fieldMapping, encodedValue.length);
        this.infoBytes.put(tokenMask);
        this.fieldCount++;
      } else {
        DataRowKeyFieldMapping dataFieldConfig = (DataRowKeyFieldMapping) fieldMapping;

        List<ScanLiteral> scanLiterals = this.scanLiterals.getLiterals(dataFieldConfig);
        // if no literal present, create a fuzzy wildcard
        // as per the field length
        if (scanLiterals == null || scanLiterals.size() == 0) {
          byte[] wildcard = new byte[dataFieldConfig.getMaxLength()];
          Arrays.fill(wildcard, (byte) Character.valueOf('*').charValue());
          this.keyBytes.put(wildcard); // no need for padding, we are
          // at max len
          byte[] fieldMask = new byte[dataFieldConfig.getMaxLength()];
          for (int i = 0; i < fieldMask.length; i++)
            fieldMask[i] = this.variableMaskByte;
          this.infoBytes.put(fieldMask);
        } else {
          if (scanLiterals.size() > 1)
            log.warn("expected single literal for data field - ignoring");
          FuzzyRowKeyLiteral fuzzyLiteral = (FuzzyRowKeyLiteral) scanLiterals.get(0);
          if (WildcardStringLiteral.class.isAssignableFrom(fuzzyLiteral.getClass())) {
            WildcardStringLiteral wc = (WildcardStringLiteral) fuzzyLiteral;
            String literal = wc.getContent();
            if (!literal.equals(Wildcard.WILDCARD_CHAR)) {
              if (literal.length() != wc.getFieldConfig().getMaxLength()) {
                if (literal.startsWith(Wildcard.WILDCARD_CHAR)
                    && literal.endsWith(Wildcard.WILDCARD_CHAR)) {
                  throw new InvalidFuzzyRowKeyWildcardException(
                      "wildcard expression length does not equal field max length ("
                          + wc.getFieldConfig().getMaxLength()
                          + ") for field '"
                          + wc.getFieldConfig().getEndpointProperty().getName()
                          + "' - fuzzy row key scan wildcard expressions, starting"
                          + " and ending with a wildcard character, must exactly match (typically by padding with wildcards) the length of the target (composite)"
                          + " row key field, as HBase fuzzy row key filters use fixed length masking.");
                }
              }
            }
          }

          this.keyBytes.put(fuzzyLiteral.getFuzzyKeyBytes());
          this.infoBytes.put(fuzzyLiteral.getFuzzyInfoBytes());
        }

        this.fieldCount++;
      }
    }
  }

  private byte[] getEncodedKeyField(KeyFieldMapping metaField) {
    Object keyValue = metaField.getKey(this.rootType);
    byte[] tokenValue = metaField.getCodec().encode(keyValue);
    return tokenValue;
  }

  private byte[] getKeyFieldMask(KeyFieldMapping metaField, int paddedTokenValueLength) {
    byte[] tokenMask = new byte[paddedTokenValueLength];
    Arrays.fill(tokenMask, fixedMaskByte);
    return tokenMask;
  }

  @Override
  public byte[] getFuzzyKeyBytes() {
    if (this.keyBytes == null)
      throw new IllegalStateException("row keys not assembled - first call assemble(...)");
    byte[] result = new byte[this.keyBytes.position()];
    System.arraycopy(this.keyBytes.array(), this.keyBytes.arrayOffset(), result, 0,
        this.keyBytes.position());
    return result;
  }

  @Override
  public byte[] getFuzzyInfoBytes() {
    if (this.infoBytes == null)
      throw new IllegalStateException("row keys not assembled - first call assemble(...)");
    byte[] result = new byte[this.infoBytes.position()];
    System.arraycopy(this.infoBytes.array(), this.infoBytes.arrayOffset(), result, 0,
        this.infoBytes.position());
    return result;
  }

  @Override
  public void clear() {
    this.scanLiterals = null;
  }

}
