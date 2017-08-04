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
package org.cloudgraph.config;

import java.nio.charset.Charset;

import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.core.CoreConstants;

import commonj.sdo.DataObject;

/**
 * The configuration for a row or column key.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public abstract class KeyFieldConfig {
  protected int sequenceNum;
  /** the total number of fields in the row or column composite key */
  protected int totalFields;
  protected Charset charset = Charset.forName(CoreConstants.UTF8_ENCODING);
  private KeyField field;

  @SuppressWarnings("unused")
  private KeyFieldConfig() {
  }

  public KeyFieldConfig(KeyField field, int sequenceNum, int totalFields) {
    super();
    this.field = field;
    this.sequenceNum = sequenceNum;
    this.totalFields = totalFields;
  }

  public int getSeqNum() {
    return sequenceNum;
  }

  public int getTotalFields() {
    return totalFields;
  }

  public boolean isHash() {
    return this.field.isHash();
  }

  /**
   * Returns a key value as string from the given data graph
   * 
   * @param dataGraph
   *          the data graph
   * @return the key value
   */
  public abstract String getKey(commonj.sdo.DataGraph dataGraph);

  /**
   * Returns a key value as string from the given data object
   * 
   * @param dataObject
   *          the root data object
   * @return the key value
   */
  public abstract String getKey(DataObject dataObject);

  /**
   * Returns a key value as bytes from the given data graph
   * 
   * @param dataGraph
   *          the data graph
   * @return the key value
   */
  public abstract byte[] getKeyBytes(commonj.sdo.DataGraph dataGraph);

  /**
   * Returns a key value as bytes from the given data object
   * 
   * @param dataObject
   *          the root data object
   * @return the key value
   */
  public abstract byte[] getKeyBytes(DataObject dataObject);

  /**
   * Returns the maximum length allowed for this key field.
   * 
   * @return the maximum length allowed for this key field.
   */
  public abstract int getMaxLength();

  public abstract DataFlavor getDataFlavor();

}
