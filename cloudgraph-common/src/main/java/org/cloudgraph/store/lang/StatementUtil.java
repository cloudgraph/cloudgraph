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
package org.cloudgraph.store.lang;

import java.util.List;
import java.util.Map;

import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.profile.KeyType;

import commonj.sdo.Property;

/**
 * Common graph assembler functionality resulting from initial re-factoring and
 * addition of parallel assembly across RDB and Cassandra services.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public class StatementUtil {

  public PlasmaProperty getOppositePriKeyProperty(Property targetProperty) {
    PlasmaProperty opposite = (PlasmaProperty) targetProperty.getOpposite();
    PlasmaType oppositeType = null;

    if (opposite != null) {
      oppositeType = (PlasmaType) opposite.getContainingType();
    } else {
      oppositeType = (PlasmaType) targetProperty.getType();
    }

    List<Property> pkeyProps = oppositeType.findProperties(KeyType.primary);
    if (pkeyProps.size() == 0) {
      throw new DataAccessException("no opposite pri-key properties found"
          + " - cannot map from reference property, " + targetProperty.toString());
    }
    PlasmaProperty supplier = ((PlasmaProperty) targetProperty).getKeySupplier();
    if (supplier != null) {
      return supplier;
    } else if (pkeyProps.size() == 1) {
      return (PlasmaProperty) pkeyProps.get(0);
    } else {
      throw new DataAccessException("multiple opposite pri-key properties found"
          + " - cannot map from reference property, " + targetProperty.toString()
          + " - please add a derivation supplier");
    }
  }

  public boolean hasUpdatableProperties(Map<String, PropertyPair> values) {

    for (PropertyPair pair : values.values()) {
      PlasmaProperty prop = pair.getProp();
      if (prop.isMany() && !prop.getType().isDataType())
        continue; // no such thing as updatable many reference property
      // in RDBMS
      if (prop.isKey(KeyType.primary))
        if (pair.getOldValue() == null) // key not modified, we're not
          // updating it
          continue;
      return true;
    }
    return false;
  }

  public String getQualifiedPhysicalName(PlasmaType type) {
    String packageName = type.getPackagePhysicalName();
    if (packageName != null)
      return packageName + "." + type.getPhysicalName();
    else
      return type.getPhysicalName();
  }

}
