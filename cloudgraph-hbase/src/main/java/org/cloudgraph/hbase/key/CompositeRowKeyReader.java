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

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Hash;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.KeyFieldConfig;
import org.cloudgraph.config.PreDefinedKeyFieldConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.service.HBaseDataConverter;
import org.cloudgraph.recognizer.Endpoint;
import org.cloudgraph.state.RowState;
import org.cloudgraph.store.key.KeyValue;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.helper.DataConverter;

import commonj.sdo.Property;

/**
 * Reads and parses row keys into {@link KeyValue} elements where the values can
 * then be found using a given {@link Endpoint} and used for syntax tree
 * evaluation, for example.
 * <p>
 * </p>
 * For a collection of row keys, for every key in the collection, a client will
 * read a single row key, then collect its values. Endpoints are created on the
 * first row key and used for all additional keys.
 * 
 * @author Scott Cinnamond
 * @since 1.0.4
 * 
 * @see Endpoint
 * @see KeyValue
 * 
 */
public class CompositeRowKeyReader {
  private static final Log log = LogFactory.getLog(CompositeRowKeyReader.class);
  private TableConfig table;
  private DataGraphConfig graph;
  private PlasmaType contextType;
  private char rowKeyFieldDelimChar;
  private Map<UserDefinedRowKeyFieldConfig, Endpoint> endpointMap;
  private Map<Endpoint, KeyValue> valueMap;

  @SuppressWarnings("unused")
  private CompositeRowKeyReader() {
  }

  public CompositeRowKeyReader(PlasmaType contextType) {
    if (contextType == null)
      throw new IllegalArgumentException("expected arg contextType");
    this.contextType = contextType;
    this.table = CloudGraphConfig.getInstance().getTable(this.contextType);
    this.graph = CloudGraphConfig.getInstance().getDataGraph(this.contextType.getQualifiedName());

    this.valueMap = new HashMap<>();
    this.endpointMap = new HashMap<>();
    if (this.graph.getRowKeyFieldDelimiter().length() == 1) {
      this.rowKeyFieldDelimChar = this.graph.getRowKeyFieldDelimiter().charAt(0);
    } else
      throw new IllegalStateException("expected single character delimiter, not, '"
          + this.graph.getRowKeyFieldDelimiter() + "'");

    construct();
  }

  private void construct() {
    for (KeyFieldConfig keyField : this.graph.getRowKeyFields()) {
      if (PreDefinedKeyFieldConfig.class.isAssignableFrom(keyField.getClass())) {
        if (log.isDebugEnabled())
          log.debug("ignoring predefined field config, "
              + ((PreDefinedKeyFieldConfig) keyField).getName());
        continue;
      }
      UserDefinedRowKeyFieldConfig userDefinedKeyField = (UserDefinedRowKeyFieldConfig) keyField;
      PlasmaProperty endpointProp = userDefinedKeyField.getEndpointProperty();
      if (keyField.isHash()) {
        log.warn("cannot unmarshal hashed row key field for table, " + this.table.getName()
            + ", with graph, " + this.graph + ", and endpoint property, " + endpointProp
            + " - continuing");
        continue;
      }
      Endpoint endpoint = this.endpointMap.get(userDefinedKeyField);
      if (endpoint == null) {
        endpoint = new Endpoint(endpointProp, userDefinedKeyField.getPropertyPath());
        this.endpointMap.put(userDefinedKeyField, endpoint);
      }
    }

  }

  public Collection<Endpoint> getEndpoints() {
    return this.endpointMap.values();
  }

  public TableConfig getTable() {
    return table;
  }

  public DataGraphConfig getGraph() {
    return graph;
  }

  public PlasmaType getContextType() {
    return contextType;
  }

  public void read(String rowKey) {
    this.valueMap.clear();
    Iterator<String> iter = fastSplit(rowKey, this.rowKeyFieldDelimChar).iterator();
    int i = 0;
    while (iter.hasNext()) {
      String token = iter.next().trim();
      KeyFieldConfig keyField = this.graph.getRowKeyFields().get(i);
      if (PreDefinedKeyFieldConfig.class.isAssignableFrom(keyField.getClass())) {
        if (log.isDebugEnabled())
          log.debug("ignoring predefined field config(" + i + "), "
              + ((PreDefinedKeyFieldConfig) keyField).getName());
        continue;
      }
      UserDefinedRowKeyFieldConfig userDefinedKeyField = (UserDefinedRowKeyFieldConfig) keyField;
      PlasmaProperty endpointProp = userDefinedKeyField.getEndpointProperty();
      if (keyField.isHash()) {
        log.warn("cannot unmarshal hashed row key field(" + i + ") for table, "
            + this.table.getName() + ", with graph, " + this.graph + ", and endpoint property, "
            + endpointProp + " - continuing");
        continue;
      }
      Endpoint endpoint = this.endpointMap.get(userDefinedKeyField);
      Object value = null;
      DataType sdoType = DataType.valueOf(endpointProp.getType().getName());
      switch (sdoType) {
      case String: // no conversion
        value = token;
        break;
      default:
        value = DataConverter.INSTANCE.fromString(endpointProp, token);
        break;
      }
      KeyValue kv = new KeyValue(endpointProp, value);
      kv.setPropertyPath(userDefinedKeyField.getPropertyPath());
      if (this.valueMap.containsKey(endpoint))
        throw new IllegalStateException("expected single value for endpoint, " + endpoint);
      this.valueMap.put(endpoint, kv);
      i++;
    }
  }

  /**
   * String split which beats Java regex String.split() by nearly an order of
   * magnitude.
   * 
   * @param str
   *          the source string
   * @param delim
   *          the delimiter
   * @return the tokens
   */
  private List<String> fastSplit(String str, char delim) {
    char[] chars = str.toCharArray();
    LinkedList<String> list = new LinkedList<>();
    int i, index = 0;
    for (i = 0; i < chars.length; i++) {
      if (chars[i] == delim) {
        list.add(str.substring(index, i));
        index = i + 1;
      }
    }
    if (i > index)
      list.add(str.substring(index, i));
    return list;
  }

  public Object getValue(Endpoint endpoint) {
    KeyValue kv = this.valueMap.get(endpoint);
    if (kv != null)
      return kv.getValue();
    return null;
  }

  public Collection<KeyValue> getValues() {
    return this.valueMap.values();
  }

  private boolean isSpecialRegexChar(char c) {
    switch (c) {
    case '|':
    case '*':
      return true;
    default:
      return false;
    }
  }
}
