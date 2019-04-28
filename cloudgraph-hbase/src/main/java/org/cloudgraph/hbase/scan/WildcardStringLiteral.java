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

import org.cloudgraph.common.Padding;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.Wildcard;
import org.plasma.query.model.PredicateOperator;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaType;

/**
 * A wildcard string data "flavor" specific literal class used to abstract the
 * complexities involved in assembling the various segments and fields of
 * composite (scan start/stop) row keys under various relational and logical
 * operator and various configurable composite key-field hashing, formatting,
 * padding and other features.
 * 
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.hbase.service.HBaseDataConverter
 * @author Scott Cinnamond
 * @since 0.5
 */
public class WildcardStringLiteral extends StringLiteral implements WildcardPartialRowKeyLiteral,
    FuzzyRowKeyLiteral {

  @Deprecated
  private PredicateOperator wildcardOperator;
  protected Padding padding;

  public WildcardStringLiteral(String literal, PlasmaType rootType,
      PredicateOperator wildcardOperator, DataRowKeyFieldMapping fieldConfig,
      StoreMappingContext mappingContext) {
    super(literal, rootType, null, fieldConfig, mappingContext);
    this.wildcardOperator = wildcardOperator;
    this.padding = new Padding(this.charset);
  }

  public PredicateOperator getWildcardOperator() {
    return wildcardOperator;
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
  @Override
  public byte[] getStartBytes() {
    switch (this.wildcardOperator.getValue()) {
    case LIKE:
      return getBetweenStartBytes();
    default:
      throw new GraphServiceException("wildcard operator '"
          + this.wildcardOperator.getValue().toString()
          + "' not supported for string row key fields");
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
  @Override
  public byte[] getStopBytes() {
    switch (this.wildcardOperator.getValue()) {
    case LIKE:
      return getBetweenStopBytes();
    default:
      throw new GraphServiceException("wildcard operator '"
          + this.wildcardOperator.getValue().toString()
          + "' not supported for string row key fields");
    }
  }

  @Override
  public byte[] getFuzzyKeyBytes() {
    byte[] keyBytes = null;
    String keyValueStr = this.literal.trim();
    switch (fieldConfig.getCodecType()) {
    case HASH:
      throw new ScanException("cannot create fuzzy scan literal "
          + "with interviening wildcards ('" + keyValueStr
          + "') for fixed length row key field with path '" + this.fieldConfig.getPropertyPath()
          + "' within table " + this.table.getQualifiedPhysicalName() + " for graph root type, "
          + this.rootType.toString());
    default:
      if (keyValueStr.contains(Wildcard.WILDCARD_CHAR)) {
        if (keyValueStr.endsWith(Wildcard.WILDCARD_CHAR)) {
          keyValueStr = this.padding.back(keyValueStr, this.fieldConfig.getMaxLength(),
              Wildcard.WILDCARD_CHAR.charAt(0));
        } else if (keyValueStr.startsWith(Wildcard.WILDCARD_CHAR)) {
          keyValueStr = this.padding.front(keyValueStr, this.fieldConfig.getMaxLength(),
              Wildcard.WILDCARD_CHAR.charAt(0));
        } else { // key fields are fixed length - intervening wildcard
          // cannot work
          throw new ScanException("cannot create fuzzy scan literal "
              + "with interviening wildcards ('" + keyValueStr
              + "') for fixed length row key field with path '"
              + this.fieldConfig.getPropertyPath() + "' within table "
              + this.table.getQualifiedPhysicalName() + " for graph root type, "
              + this.rootType.toString());
        }
      } else {
        keyValueStr = this.padding.pad(keyValueStr, this.fieldConfig.getMaxLength(),
            DataFlavor.string);
      }
      keyBytes = keyValueStr.getBytes(this.charset);
      break;
    }
    return keyBytes;
  }

  @Override
  public byte[] getFuzzyInfoBytes() {
    byte[] infoBytes = new byte[this.fieldConfig.getMaxLength()];
    byte[] literalChars = getFuzzyKeyBytes();
    char wildcard = Wildcard.WILDCARD_CHAR.toCharArray()[0];
    for (int i = 0; i < infoBytes.length; i++)
      if (literalChars[i] != wildcard)
        infoBytes[i] = (byte) 0; // fixed char
      else
        infoBytes[i] = (byte) 1; // fuzzy char
    return infoBytes;
  }

  /**
   * Returns the "start row" bytes used under certain conditions to represent a
   * "wildcard" operator under an HBase partial row-key scan under the various
   * optionally configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used under certain conditions to represent a
   *         "wildcard" operator under an HBase partial row-key scan under the
   *         various optionally configurable hashing, formatting and padding
   *         features.
   */
  @Override
  public byte[] getBetweenStartBytes() {
    byte[] startBytes = null;
    String startValueStr = this.literal.substring(0, this.literal.length() - 1);
    switch (fieldConfig.getCodecType()) {
    case HASH:
      throw new ScanException("cannot create scan literal "
          + "for hashed key field - field with path '" + this.fieldConfig.getPropertyPath()
          + "' within table " + this.table.getQualifiedPhysicalName() + " for graph root type, "
          + this.rootType.toString());
    default:
      startBytes = startValueStr.getBytes(this.charset);
      startBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          this.fieldConfig.getDataFlavor());
      break;
    }
    return startBytes;
  }

  /**
   * Returns the "stop row" bytes used under certain conditions to represent a
   * "wildcard" operator under an HBase partial row-key scan under the various
   * optionally configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used under certain conditions to represent a
   *         "wildcard" operator under an HBase partial row-key scan under the
   *         various optionally configurable hashing, formatting and padding
   *         features.
   */
  @Override
  public byte[] getBetweenStopBytes() {
    byte[] stopBytes = null;
    String stopValueStr = this.literal.substring(0, this.literal.length() - 1);

    switch (fieldConfig.getCodecType()) {
    case HASH:
      throw new ScanException("cannot create scan literal "
          + "for hashed key field - field with path '" + this.fieldConfig.getPropertyPath()
          + "' within table " + this.table.getQualifiedPhysicalName() + " for graph root type, "
          + this.rootType.toString() + "is configured as 'hashed'");
    default:
      byte[] literalStopBytes = stopValueStr.getBytes(this.charset);
      stopBytes = new byte[literalStopBytes.length + 1];
      System.arraycopy(literalStopBytes, 0, stopBytes, 0, literalStopBytes.length);
      stopBytes[stopBytes.length - 1] = INCREMENT;
      stopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(),
          this.fieldConfig.getDataFlavor());
      break;
    }
    return stopBytes;
  }

}
