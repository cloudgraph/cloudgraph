/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
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
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.KeyFieldConfig;
import org.cloudgraph.config.PreDefinedKeyFieldConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.key.Hashing;
import org.cloudgraph.hbase.key.KeySupport;
import org.cloudgraph.hbase.key.Padding;
import org.plasma.query.model.Where;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaType;

/**
 * Assembles a row key where each field within the composite row key is
 * constructed based a set of given query predicates.
 * 
 * @see org.cloudgraph.config.DataGraphConfig
 * @see org.cloudgraph.config.TableConfig
 * @see org.cloudgraph.config.UserDefinedField
 * @see org.cloudgraph.config.PredefinedField
 * @see org.cloudgraph.config.PreDefinedFieldName
 * @author Scott Cinnamond
 * @since 0.5.5
 */
public class CompleteRowKeyAssembler implements RowKeyScanAssembler, CompleteRowKey {
  private static Log log = LogFactory.getLog(CompleteRowKeyAssembler.class);
  protected int bufsize = 4000;
  protected ByteBuffer startKey = ByteBuffer.allocate(bufsize);
  protected PlasmaType rootType;
  protected DataGraphConfig graph;
  protected TableConfig table;
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
    this.graph = CloudGraphConfig.getInstance().getDataGraph(rootTypeQname);
    this.table = CloudGraphConfig.getInstance().getTable(rootTypeQname);
    Hash hash = this.keySupport.getHashAlgorithm(this.table);
    this.charset = CloudGraphConfig.getInstance().getCharset();
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
   * @see org.cloudgraph.config.PredefinedField
   * @see org.cloudgraph.config.PreDefinedFieldName
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
   * @see org.cloudgraph.config.PredefinedField
   * @see org.cloudgraph.config.PreDefinedFieldName
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
    List<PreDefinedKeyFieldConfig> resultFields = new ArrayList<PreDefinedKeyFieldConfig>();

    for (PreDefinedKeyFieldConfig field : this.graph.getPreDefinedRowKeyFields()) {
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
      PreDefinedKeyFieldConfig preDefinedField = resultFields.get(i);
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
    for (KeyFieldConfig fieldConfig : this.graph.getRowKeyFields()) {
      if (fieldConfig instanceof PreDefinedKeyFieldConfig) {
        PreDefinedKeyFieldConfig predefinedConfig = (PreDefinedKeyFieldConfig) fieldConfig;

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
      } else if (fieldConfig instanceof UserDefinedRowKeyFieldConfig) {
        UserDefinedRowKeyFieldConfig userFieldConfig = (UserDefinedRowKeyFieldConfig) fieldConfig;
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
