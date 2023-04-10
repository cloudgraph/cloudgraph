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
package org.cloudgraph.core.key;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.recognizer.Endpoint;
import org.cloudgraph.recognizer.RecognizerEndpoint;
import org.cloudgraph.store.key.KeyValue;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.KeyFieldCodecType;
import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.cloudgraph.store.mapping.MetaKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

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
  private TableMapping table;
  private DataGraphMapping graph;
  private PlasmaType contextType;
  private char rowKeyFieldDelimChar;
  private Map<DataRowKeyFieldMapping, Endpoint> endpointMap;
  private Map<Endpoint, KeyValue> valueMap;
  private StoreMappingContext mappingContext;

  @SuppressWarnings("unused")
  private CompositeRowKeyReader() {
  }

  public CompositeRowKeyReader(PlasmaType contextType, StoreMappingContext mappingContext) {
    if (contextType == null)
      throw new IllegalArgumentException("expected arg contextType");
    this.contextType = contextType;
    this.mappingContext = mappingContext;
    this.table = StoreMapping.getInstance().getTable(this.contextType, this.mappingContext);
    this.graph = StoreMapping.getInstance().getDataGraph(this.contextType.getQualifiedName(),
        this.mappingContext);

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
    for (KeyFieldMapping keyField : this.graph.getRowKeyFields()) {
      if (MetaKeyFieldMapping.class.isAssignableFrom(keyField.getClass())) {
        if (log.isDebugEnabled())
          log.debug("ignoring predefined field config, "
              + ((MetaKeyFieldMapping) keyField).getName());
        continue;
      }
      DataRowKeyFieldMapping userDefinedKeyField = (DataRowKeyFieldMapping) keyField;
      PlasmaProperty endpointProp = userDefinedKeyField.getEndpointProperty();
      if (keyField.getCodecType().ordinal() == KeyFieldCodecType.HASH.ordinal()) {
        log.warn("cannot unmarshal hashed row key field for table, "
            + this.table.getNamespaceQualifiedPhysicalName() + ", with graph, " + this.graph
            + ", and endpoint property, " + endpointProp + " - continuing");
        continue;
      }
      Endpoint endpoint = this.endpointMap.get(userDefinedKeyField);
      if (endpoint == null) {
        endpoint = new RecognizerEndpoint(endpointProp, userDefinedKeyField.getPropertyPath());
        this.endpointMap.put(userDefinedKeyField, endpoint);
      }
    }

  }

  public Collection<Endpoint> getEndpoints() {
    return this.endpointMap.values();
  }

  public TableMapping getTable() {
    return table;
  }

  public DataGraphMapping getGraph() {
    return graph;
  }

  public PlasmaType getContextType() {
    return contextType;
  }

  public void read(byte[] rowKey) {
    this.valueMap.clear();
    Iterator<byte[]> iter = fastSplit(rowKey, this.rowKeyFieldDelimChar).iterator();
    int i = 0;
    while (iter.hasNext()) {
      byte[] token = iter.next();
      KeyFieldMapping keyField = this.graph.getRowKeyFields().get(i);
      if (MetaKeyFieldMapping.class.isAssignableFrom(keyField.getClass())) {
        if (log.isDebugEnabled())
          log.debug("ignoring predefined field config(" + i + "), "
              + ((MetaKeyFieldMapping) keyField).getName());
        continue;
      }
      DataRowKeyFieldMapping userDefinedKeyField = (DataRowKeyFieldMapping) keyField;
      PlasmaProperty endpointProp = userDefinedKeyField.getEndpointProperty();
      if (keyField.getCodecType().ordinal() == KeyFieldCodecType.HASH.ordinal()) {
        log.warn("cannot unmarshal hashed row key field(" + i + ") for table, "
            + this.table.getNamespaceQualifiedPhysicalName() + ", with graph, " + this.graph
            + ", and endpoint property, " + endpointProp + " - continuing");
        continue;
      }
      Object value = keyField.getCodec().decode(token);
      Endpoint endpoint = this.endpointMap.get(userDefinedKeyField);

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

  private List<byte[]> fastSplit(byte[] chars, char delim) {
    LinkedList<byte[]> list = new LinkedList<>();
    int i, index = 0;
    for (i = 0; i < chars.length; i++) {
      if (chars[i] == delim) {
        byte[] token = new byte[i - index];
        System.arraycopy(chars, index, token, 0, token.length);
        list.add(token);
        index = i + 1;
      }
    }
    if (i > index) {
      if (index > 0) {
        byte[] token = new byte[i - index];
        System.arraycopy(chars, index, token, 0, token.length);
        list.add(token);
      } else {
        list.add(chars);
      }
    }
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
