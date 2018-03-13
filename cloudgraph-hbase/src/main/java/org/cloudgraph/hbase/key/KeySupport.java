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
package org.cloudgraph.hbase.key;

import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.common.hash.Hash;
import org.cloudgraph.common.hash.JenkinsHash;
import org.cloudgraph.common.hash.MurmurHash;
import org.cloudgraph.hbase.scan.StringLiteral;
import org.cloudgraph.hbase.service.CloudGraphContext;
import org.cloudgraph.store.key.KeyValue;
import org.cloudgraph.store.mapping.PreDefinedKeyFieldMapping;
import org.cloudgraph.store.mapping.PredefinedField;
import org.cloudgraph.store.mapping.StoreMappingException;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.mapping.UserDefinedRowKeyFieldMapping;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;

/**
 * Delegate class supporting composite key generation.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class KeySupport {
  private static final Log log = LogFactory.getLog(CompositeRowKeyFactory.class);

  public KeyValue findKeyValue(UserDefinedRowKeyFieldMapping fieldConfig, List<KeyValue> pairs) {

    PlasmaProperty fieldProperty = fieldConfig.getEndpointProperty();

    for (KeyValue keyValue : pairs) {
      if (keyValue.getProp().equals(fieldProperty)) {
        if (fieldConfig.getPropertyPath() != null && keyValue.getPropertyPath() != null) {
          if (keyValue.getPropertyPath().equals(fieldConfig.getPropertyPath())) {
            return keyValue;
          }
        }
      }
    }
    return null;
  }

}
