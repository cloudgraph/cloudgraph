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
package org.cloudgraph.recognizer;

import org.plasma.query.model.AbstractPathElement;
import org.plasma.query.model.Path;
import org.plasma.query.model.PathElement;
import org.plasma.query.model.Property;
import org.plasma.query.model.WildcardPathElement;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * An property qualified by its path from the root of a graph, its type and its
 * unique identifier.
 * 
 * @author Scott Cinnamond
 * @since 1.0.4
 */
public class Endpoint {
  private PlasmaProperty property;
  private String path;
  private int hashCode = 0;

  @SuppressWarnings("unused")
  private Endpoint() {
  }

  /**
   * Constructor which calculates the endpoint for the given query property for
   * the given initial (root) type, which searches for the endpoint by
   * traversing the metadata references.
   * 
   * @param property
   *          the query property which may or may not contain a path
   * @param rootType
   *          the root type
   */
  public Endpoint(Property property, PlasmaType rootType) {
    StringBuilder buf = new StringBuilder();
    Path path = property.getPath();
    PlasmaType targetType = rootType;
    if (path != null)
      for (int i = 0; i < path.getPathNodes().size(); i++) {
        if (i > 0)
          buf.append("/");
        AbstractPathElement pathElem = path.getPathNodes().get(i).getPathElement();
        if (pathElem instanceof WildcardPathElement)
          throw new IllegalStateException(
              "wildcard path elements applicable for 'Select' clause paths only, not 'Where' clause paths");
        String elem = ((PathElement) pathElem).getValue();
        buf.append(elem);
        PlasmaProperty prop = (PlasmaProperty) targetType.getProperty(elem);
        targetType = (PlasmaType) prop.getType(); // traverse
      }

    this.property = (PlasmaProperty) targetType.getProperty(property.getName());
    this.path = buf.toString();
    if (this.path.length() == 0)
      this.path = null;
  }

  public Endpoint(PlasmaProperty endpoint, String path) {
    this.property = endpoint;
    if (path != null && path.contains("/")) {
      this.path = path.substring(0, path.lastIndexOf("/") + 1);
    } else
      this.path = null;
    if (this.path != null && this.path.length() == 0)
      this.path = null;
  }

  public int hashCode() {
    if (this.hashCode != 0)
      return this.hashCode;
    final int prime = 31;
    this.hashCode = 1;
    if (this.path != null)
      this.hashCode = prime * this.hashCode + this.path.hashCode();
    this.hashCode = prime * this.hashCode + this.property.getId().hashCode();
    return this.hashCode;
  }

  public boolean equals(Object other) {
    if (other != null) {
      Endpoint otherType = (Endpoint) other;
      return this.hashCode() == otherType.hashCode();
    }
    return false;
  }

  public PlasmaProperty getProperty() {
    return property;
  }

  public boolean hasPath() {
    return path.length() > 0;
  }

  public String getPath() {
    return path;
  }

  public String toString() {
    if (hasPath())
      return getPath() + "/" + String.valueOf(this.property);
    else
      return String.valueOf(this.property);
  }
}
