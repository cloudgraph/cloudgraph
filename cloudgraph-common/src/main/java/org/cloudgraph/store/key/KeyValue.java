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
package org.cloudgraph.store.key;

import org.plasma.query.model.QueryConstants;
import org.plasma.sdo.PlasmaProperty;

/**
 * Associates an SDO property with a data value and optional path to the
 * property from a given context root.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class KeyValue {
  private PlasmaProperty prop;
  private String propertyPath;
  private Object value;
  private boolean isWildcard = false;
  private String wildcard = QueryConstants.WILDCARD;

  @SuppressWarnings("unused")
  private KeyValue() {
  }

  public KeyValue(PlasmaProperty prop, Object value) {
    this.prop = prop;
    this.value = value;
  }

  public PlasmaProperty getProp() {
    return prop;
  }

  public Object getValue() {
    return value;
  }

  public String getPropertyPath() {
    return propertyPath;
  }

  public void setPropertyPath(String propertyPath) {
    this.propertyPath = propertyPath;
  }

  public boolean isWildcard() {
    return isWildcard;
  }

  public void setIsWildcard(boolean isWildcard) {
    this.isWildcard = isWildcard;
  }

  public String getWildcard() {
    return wildcard;
  }

  public void setWildcard(String wildcard) {
    this.wildcard = wildcard;
  }

  public String toString() {

    if (this.propertyPath != null)
      return this.propertyPath + ":" + this.prop + "/" + String.valueOf(this.value);
    else
      return this.prop + "/" + String.valueOf(this.value);
  }
}
