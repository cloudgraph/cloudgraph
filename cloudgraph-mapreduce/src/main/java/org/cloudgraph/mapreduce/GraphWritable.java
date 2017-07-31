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
package org.cloudgraph.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.plasma.sdo.helper.PlasmaXMLHelper;
import org.plasma.sdo.xml.DefaultOptions;

import commonj.sdo.DataGraph;
import commonj.sdo.helper.XMLDocument;

/**
 * Allows data graphs to be consumable by Hadoop using XML serialization under
 * standard SDO provided mechanisms. A data graph of any depth or complexity may
 * be represented including graphs where the underlying model contains instances
 * of multiple inheritance. No XML Schema is required as the serialized form is
 * used as an internal representation only, and no XML Schema validation is
 * performed.
 * 
 * <p>
 * For the write operation, the root URI, prepended by an integer representing
 * its length, is written out first as this is critical for de-serialization in
 * some cases. Then the XML representation is written, also prepended by an
 * integer representing its length. During the read operation, the root URI is
 * first un-marshaled and then used as an option for XML de-serialization.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 * @see commonj.sdo.DataGraph
 * @see commonj.sdo.helper.XMLDocument
 */
public class GraphWritable implements Writable {

  private DataGraph dataGraph;

  public GraphWritable() {
    // for serialization only
  }

  public GraphWritable(DataGraph dataGraph) {
    this.dataGraph = dataGraph;
  }

  /**
   * Returns a
   * 
   * @return
   */
  public DataGraph getDataGraph() {
    return dataGraph;
  }

  /**
   * The root URI, prepended by an integer representing its length, is written
   * out first as this is critical for de-serialization in some cases. Then the
   * XML representation is written, also prepended by an integer representing
   * its length.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    String uri = this.dataGraph.getRootObject().getType().getURI();
    byte[] uribytes = uri.getBytes();
    out.writeInt(uribytes.length);
    out.write(uribytes);

    byte[] bytes = serializeGraph(this.dataGraph);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  /**
   * The root URI is first unmarshaled and then used as an option for XML
   * de-serialization. (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == 0) {
      return;
    }
    byte[] buf = new byte[length];
    in.readFully(buf);
    String uri = new String(buf);

    length = in.readInt();
    if (length == 0) {
      return;
    }
    buf = new byte[length];
    in.readFully(buf);

    this.dataGraph = deserializeGraph(buf, uri);
  }

  public String toXMLString() throws IOException {
    return new String(serializeGraph(this.dataGraph));
  }

  private DataGraph deserializeGraph(byte[] buf, String uri) throws IOException {
    long before = System.currentTimeMillis();

    ByteArrayInputStream is = new ByteArrayInputStream(buf);
    DefaultOptions options = new DefaultOptions(uri);
    options.setRootNamespacePrefix("ns1");
    options.setValidate(false); // no XML schema for the doc necessary or
    // present
    XMLDocument doc = PlasmaXMLHelper.INSTANCE.load(is, uri, options);

    long after = System.currentTimeMillis();
    // System.out.println(GraphWritable.class.getSimpleName() +
    // " deserialization: " + String.valueOf(after - before));

    return doc.getRootObject().getDataGraph();
  }

  private byte[] serializeGraph(DataGraph graph) throws IOException {
    long before = System.currentTimeMillis();
    DefaultOptions options = new DefaultOptions(graph.getRootObject().getType().getURI());
    options.setRootNamespacePrefix("ns1");
    // options.setPrettyPrint(false);
    XMLDocument doc = PlasmaXMLHelper.INSTANCE.createDocument(graph.getRootObject(), graph
        .getRootObject().getType().getURI(), null);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PlasmaXMLHelper.INSTANCE.save(doc, os, options);
    os.flush();
    long after = System.currentTimeMillis();
    // System.out.println(GraphWritable.class.getSimpleName() +
    // " serialization: " + String.valueOf(after - before));
    return os.toByteArray();
  }
}
