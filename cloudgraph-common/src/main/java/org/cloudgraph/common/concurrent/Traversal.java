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
package org.cloudgraph.common.concurrent;

import java.util.List;

import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.PropertyPair;

/**
 * Encapsulates minimal recursive graph traversal information for use in
 * concurrent contexts, such as where a traversal is needed but the
 * initialization data must be captured and stored, and a traversal from a given
 * subroot, initiated later.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public class Traversal {
  private PlasmaType subrootType;
  private PlasmaDataObject source;
  private PlasmaProperty sourceProperty;
  private List<PropertyPair> childKeyPairs;
  private int level;

  @SuppressWarnings("unused")
  private Traversal() {
  }

  public Traversal(PlasmaType subrootType, PlasmaDataObject source, PlasmaProperty sourceProperty,
      List<PropertyPair> childKeyPairs, int level) {
    super();
    this.subrootType = subrootType;
    this.source = source;
    this.sourceProperty = sourceProperty;
    this.childKeyPairs = childKeyPairs;
    this.level = level;
  }

  public PlasmaType getSubrootType() {
    return subrootType;
  }

  public PlasmaDataObject getSource() {
    return source;
  }

  public PlasmaProperty getSourceProperty() {
    return sourceProperty;
  }

  public List<PropertyPair> getChildKeyPairs() {
    return childKeyPairs;
  }

  public int getLevel() {
    return level;
  }

}
