/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
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
    return this.prop.getName() + "/" + String.valueOf(this.value);
  }
}
