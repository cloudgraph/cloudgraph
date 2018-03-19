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

import java.nio.charset.Charset;

import javax.xml.namespace.QName;

import org.cloudgraph.hbase.key.KeySupport;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
//import org.cloudgraph.store.mapping.Padding;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.helper.DataConverter;

/**
 * A configuration driven, context rich abstract class where subclasses are data
 * "flavor" and data type specific and used to abstract the complexities
 * involved in assembling the various segments and fields of composite (scan
 * start/stop) row keys under various relational and logical operator and
 * various configurable composite key-field hashing, formatting, padding and
 * other features.
 * 
 * @see org.cloudgraph.store.mapping.TableMapping
 * @author Scott Cinnamond
 * @since 0.5
 */
public abstract class ScanLiteral {
  protected final int HASH_INCREMENT = 1;

  protected String literal;
  @Deprecated
  protected RelationalOperatorName relationalOperator;
  protected DataRowKeyFieldMapping fieldConfig;
  protected DataConverter dataConverter = DataConverter.INSTANCE;
  protected PlasmaType rootType;
  protected Charset charset;
  protected TableMapping table;
  protected KeySupport keySupport = new KeySupport();
  protected PlasmaProperty property;

  // protected Padding padding;

  @SuppressWarnings("unused")
  private ScanLiteral() {
  }

  public ScanLiteral(String literal, PlasmaType rootType,
      RelationalOperatorName relationalOperator, DataRowKeyFieldMapping fieldConfig) {
    super();
    this.rootType = rootType;
    this.relationalOperator = relationalOperator;
    this.fieldConfig = fieldConfig;
    this.property = (PlasmaProperty) this.fieldConfig.getEndpointProperty();
    this.literal = literal;

    QName rootTypeQname = this.rootType.getQualifiedName();
    this.table = StoreMapping.getInstance().getTable(rootTypeQname);
    // Hash hash = this.keySupport.getHashAlgorithm(this.table);
    this.charset = StoreMapping.getInstance().getCharset();
    // this.hashing = new Hashing(hash, this.charset);
    // this.padding = new Padding(this.charset);
  }

  /**
   * Returns the string literal
   * 
   * @return the string literal
   */
  public final String getContent() {
    return literal;
  }

  /**
   * Returns the context relational operator.
   * 
   * @return the context relational operator.
   */
  public final RelationalOperatorName getRelationalOperator() {
    return relationalOperator;
  }

  /**
   * Returns the composite row-key field configuration.
   * 
   * @return the composite row-key field configuration.
   */
  public final DataRowKeyFieldMapping getFieldConfig() {
    return fieldConfig;
  }

  /**
   * Returns the HBase scan "start-row" composite row-key field bytes for this
   * literal under the various relational, logical operator and other optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the HBase scan "start-row" composite row-key field bytes for this
   *         literal under the various relational, logical operator and other
   *         optionally configurable hashing, formatting and padding features.
   */
  public byte[] getStartBytes() {
    switch (this.relationalOperator) {
    case EQUALS:
      return getEqualsStartBytes();
    case GREATER_THAN:
      return getGreaterThanStartBytes();
    case GREATER_THAN_EQUALS:
      return getGreaterThanEqualStartBytes();
    case LESS_THAN:
      return getLessThanStartBytes();
    case LESS_THAN_EQUALS:
      return getLessThanEqualStartBytes();
    case NOT_EQUALS:
    default:
      throw new GraphServiceException("relational operator '" + this.relationalOperator.toString()
          + "' not supported for integral row key fields");
    }
  }

  /**
   * Returns the HBase scan "stop-row" composite row-key field bytes for this
   * literal under the various relational, logical operator and other optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the HBase scan "stop-row" composite row-key field bytes for this
   *         literal under the various relational, logical operator and other
   *         optionally configurable hashing, formatting and padding features.
   */
  public byte[] getStopBytes() {
    switch (this.relationalOperator) {
    case EQUALS:
      return getEqualsStopBytes();
    case GREATER_THAN:
      return getGreaterThanStopBytes();
    case GREATER_THAN_EQUALS:
      return getGreaterThanEqualStopBytes();
    case LESS_THAN:
      return getLessThanStopBytes();
    case LESS_THAN_EQUALS:
      return getLessThanEqualStopBytes();
    case NOT_EQUALS:
    default:
      throw new GraphServiceException("relational operator '" + this.relationalOperator.toString()
          + "' not supported for integral row key fields");
    }
  }

  /**
   * Returns the "start row" bytes used to represent "equals" relational
   * operator under an HBase partial row-key scan for this literal under the
   * various optionally configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "equals" relational
   *         operator under an HBase partial row-key scan for this literal under
   *         the various optionally configurable hashing, formatting and padding
   *         features.
   */
  public abstract byte[] getEqualsStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "equals" relational operator
   * under an HBase partial row-key scan for this literal under the various
   * optionally configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "equals" relational operator
   *         under an HBase partial row-key scan for this literal under the
   *         various optionally configurable hashing, formatting and padding
   *         features.
   */
  public abstract byte[] getEqualsStopBytes();

  /**
   * Returns the "start row" bytes used to represent "greater than" relational
   * operator under an HBase partial row-key scan for this literal under the
   * various optionally configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "greater than" relational
   *         operator under an HBase partial row-key scan for this literal under
   *         the various optionally configurable hashing, formatting and padding
   *         features.
   */
  public abstract byte[] getGreaterThanStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "greater than" relational
   * operator under an HBase partial row-key scan for this literal under the
   * various optionally configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "greater than" relational
   *         operator under an HBase partial row-key scan for this literal under
   *         the various optionally configurable hashing, formatting and padding
   *         features.
   */
  public abstract byte[] getGreaterThanStopBytes();

  /**
   * Returns the "start row" bytes used to represent "greater than equals"
   * relational operator under an HBase partial row-key scan for this literal
   * under the various optionally configurable hashing, formatting and padding
   * features.
   * 
   * @return the "start row" bytes used to represent "greater than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public abstract byte[] getGreaterThanEqualStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "greater than equals"
   * relational operator under an HBase partial row-key scan for this literal
   * under the various optionally configurable hashing, formatting and padding
   * features.
   * 
   * @return the "stop row" bytes used to represent "greater than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public abstract byte[] getGreaterThanEqualStopBytes();

  /**
   * Returns the "start row" bytes used to represent "less than" relational
   * operator under an HBase partial row-key scan for this literal under the
   * various optionally configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "less than" relational
   *         operator under an HBase partial row-key scan for this literal under
   *         the various optionally configurable hashing, formatting and padding
   *         features.
   */
  public abstract byte[] getLessThanStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "less than" relational
   * operator under an HBase partial row-key scan for this literal under the
   * various optionally configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "less than" relational
   *         operator under an HBase partial row-key scan for this literal under
   *         the various optionally configurable hashing, formatting and padding
   *         features.
   */
  public abstract byte[] getLessThanStopBytes();

  /**
   * Returns the "start row" bytes used to represent "less than equals"
   * relational operator under an HBase partial row-key scan for this literal
   * under the various optionally configurable hashing, formatting and padding
   * features.
   * 
   * @return the "start row" bytes used to represent "less than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public abstract byte[] getLessThanEqualStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "less than equals"
   * relational operator under an HBase partial row-key scan for this literal
   * under the various optionally configurable hashing, formatting and padding
   * features.
   * 
   * @return the "stop row" bytes used to represent "less than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public abstract byte[] getLessThanEqualStopBytes();
}
