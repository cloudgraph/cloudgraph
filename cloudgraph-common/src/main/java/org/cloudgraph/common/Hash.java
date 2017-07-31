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
package org.cloudgraph.common;

/**
 * @author Scott Cinnamond
 * @since 0.5
 */
public interface Hash {
  /**
   * Calculate a hash using all bytes from the input argument, and a seed of -1.
   * Uses the hash algorithm defined for this specific HBase table or if no
   * defined, uses the hash algorithm set for the HBase configuration.
   * 
   * @param bytes
   * @return the hash value
   */
  public int hash(byte[] bytes);

  /**
   * Calculate a hash using all bytes from the input argument, and a provided
   * seed value. Uses the hash algorithm defined for this specific HBase table
   * or if no defined, uses the hash algorithm set for the HBase configuration.
   * 
   * @param bytes
   * @param initval
   * @return the hash value
   */
  public int hash(byte[] bytes, int initval);

  /**
   * Calculate a hash using bytes from 0 to length, and the provided seed value.
   * Uses the hash algorithm defined for this specific HBase table or if no
   * defined, uses the hash algorithm set for the HBase configuration.
   * 
   * @param bytes
   * @param length
   * @param initval
   * @return the hash value
   */
  public int hash(byte[] bytes, int length, int initval);

}
