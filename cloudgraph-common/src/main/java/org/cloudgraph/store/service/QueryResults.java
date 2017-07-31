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

package org.cloudgraph.store.service;

import java.util.Collection;

/**
 * Encapsulates the collection resulting from query execution along with the
 * Query object itself. Hopefully usefull where the query construction is
 * seperated off and the results are a collection rather than a single object.
 * In this event after collection iteration is complete, the query must be
 * closed via. Query.closeAll(), Query.close(), Extent.close(Iterator), or sume
 * such JDO call which frees appropriate re sources associated with query
 * execution.
 *
 */
public class QueryResults {
  private Collection collection;

  private QueryResults() {
  }

  public QueryResults(Collection collection) {
    this.collection = collection;
  }

  public Collection getResults() {
    return collection;
  }

}
