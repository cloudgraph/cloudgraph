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
package org.cloudgraph.aerospike.graph;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.aerospike.io.CellValues;
import org.cloudgraph.aerospike.io.GraphRowWriter;
import org.cloudgraph.aerospike.key.CompositeColumnKeyFactory;
import org.cloudgraph.aerospike.service.AerospikeDataConverter;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.key.KeyValue;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.sdo.Key;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.profile.KeyStructure;

/**
 * Converts a collection of {@link KeyValue} to a single {@link CellValues}
 * result.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 * 
 * @see KeyValue
 * @see AerospikeDataConverter
 * @see CellValues
 */
public class CellConverter {
  private static Log log = LogFactory.getLog(CellConverter.class);
  private PlasmaType rootType;
  private TableMapping rootTableConfig;
  private GraphColumnKeyFactory keyFactory;
  private AerospikeDataConverter hbaseConverter = AerospikeDataConverter.INSTANCE;

  public CellConverter(PlasmaType rootType, TableMapping rootTableConfig,
      StoreMappingContext mappingContext) {
    super();
    this.rootType = rootType;
    this.rootTableConfig = rootTableConfig;
    this.keyFactory = new CompositeColumnKeyFactory(rootType, mappingContext);
  }

  public CellValues convert(byte[] rowKey, Collection<KeyValue> values) {
    CellValues result = new CellValues(rowKey);

    byte[] typeQual = keyFactory.createColumnKey(this.rootType, EntityMetaKey.TYPE);
    byte[] typeValue = GraphRowWriter.encode(this.rootType);
    result.addColumn(this.rootTableConfig.getDataColumnFamilyNameBytes(), typeQual, typeValue);

    for (KeyValue keyValue : values) {
      PlasmaType endpointOwnerType = (PlasmaType) keyValue.getProp().getContainingType();
      if (endpointOwnerType.equals(this.rootType) || this.rootType.isBaseType(endpointOwnerType)) {
        Key key = keyValue.getProp().getKey();
        if (key != null
            && key.getStructure() != null
            && KeyStructure.valueOf(key.getStructure().name()).ordinal() == KeyStructure.uuid
                .ordinal()) {
          byte[] qual = keyFactory.createColumnKey(this.rootType, EntityMetaKey.UUID);
          byte[] value = hbaseConverter.toBytes(keyValue.getProp(), keyValue.getValue());
          result.addColumn(this.rootTableConfig.getDataColumnFamilyNameBytes(), qual, value);
        } else {
          byte[] qual = keyFactory.createColumnKey(this.rootType, keyValue.getProp());
          byte[] value = hbaseConverter.toBytes(keyValue.getProp(), keyValue.getValue());
          result.addColumn(this.rootTableConfig.getDataColumnFamilyNameBytes(), qual, value);
        }
      } else {
        log.warn("converting non-root value, " + keyValue);
        byte[] qual = keyFactory.createColumnKey((PlasmaType) keyValue.getProp()
            .getContainingType(), keyValue.getProp());
        byte[] value = hbaseConverter.toBytes(keyValue.getProp(), keyValue.getValue());
        result.addColumn(this.rootTableConfig.getDataColumnFamilyNameBytes(), qual, value);
      }
    }
    return result;

  }

}
