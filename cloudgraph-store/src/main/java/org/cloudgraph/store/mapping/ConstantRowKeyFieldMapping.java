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
package org.cloudgraph.store.mapping;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataObject;

/**
 * Encapsulates logic related to access of a configured constant row key field.
 * 
 * @author Scott Cinnamond
 * @since 1.1.0
 */
public class ConstantRowKeyFieldMapping extends KeyFieldMapping {
  private static Log log = LogFactory.getLog(ConstantRowKeyFieldMapping.class);

  private ConstantField constantField;

  public ConstantRowKeyFieldMapping(DataGraphMapping dataGraph, ConstantField constantField,
      int sequenceNum, int totalFields) {
    super(dataGraph, constantField, sequenceNum, totalFields);
    this.constantField = constantField;
  }

  public boolean equals(Object obj) {
    ConstantRowKeyFieldMapping other = (ConstantRowKeyFieldMapping) obj;
    return (this.sequenceNum == other.sequenceNum);
  }

  public int getSequenceNum() {
    return sequenceNum;
  }

  public DataGraphMapping getDataGraph() {
    return dataGraph;
  }

  @Override
  public Object getKey(commonj.sdo.DataGraph dataGraph) {
    return this.getKey(dataGraph.getRootObject());
  }

  @Override
  public Object getKey(DataObject dataObject) {
    return this.constantField.getValue();
  }

  @Override
  public Object getKey(PlasmaType type) {
    return this.constantField.getValue();
  }

  @Override
  public DataType getDataType() {
    return DataType.String;
  }

  @Override
  public DataFlavor getDataFlavor() {
    return DataFlavor.string;
  }

  @Override
  public int getMaxLength() {
    return constantField.getValue().length();
  }

  @Override
  public String toString() {
    return ConstantRowKeyFieldMapping.class.getSimpleName() + " [constantField=" + constantField
        + "]";
  }

}
