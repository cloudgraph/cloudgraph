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
package org.cloudgraph.state;

import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.plasma.common.bind.NonValidatingUnmarshaler;
import org.xml.sax.SAXException;

/**
 * State JAXB non-validating Binding delegate. It is crucial that this binding
 * be cached by service implementations at the appropriate level to 1.)
 * guarantee thread safety (this class is NOT thread safe) and 2.) re-use the
 * underlying JAXB context and parsed schema instance(s) across as many requests
 * as possible.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public class StateNonValidatingDataBinding implements NonValidatingDataBinding {

  private NonValidatingUnmarshaler unmarshaler;

  public static Class<?>[] FACTORIES = { org.cloudgraph.state.ObjectFactory.class, };

  public StateNonValidatingDataBinding() throws JAXBException, SAXException {
    this.unmarshaler = new NonValidatingUnmarshaler(JAXBContext.newInstance(FACTORIES));
  }

  public Class<?>[] getObjectFactories() {
    return FACTORIES;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.state.NonValidatingDataBinding#marshal(java.lang.Object)
   */
  @Override
  public String marshal(Object root) throws JAXBException {

    return unmarshaler.marshal(root);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.state.NonValidatingDataBinding#marshal(java.lang.Object,
   * java.io.OutputStream)
   */
  @Override
  public void marshal(Object root, OutputStream stream) throws JAXBException {
    unmarshaler.marshal(root, stream);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.state.NonValidatingDataBinding#marshal(java.lang.Object,
   * java.io.OutputStream, boolean)
   */
  @Override
  public void marshal(Object root, OutputStream stream, boolean formattedOutput)
      throws JAXBException {
    unmarshaler.marshal(root, stream, formattedOutput);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.state.NonValidatingDataBinding#unmarshal(java.lang.String)
   */
  @Override
  public Object unmarshal(String xml) throws JAXBException {
    return unmarshaler.unmarshal(xml);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.state.NonValidatingDataBinding#unmarshal(java.io.InputStream
   * )
   */
  @Override
  public Object unmarshal(InputStream stream) throws JAXBException {
    return unmarshaler.unmarshal(stream);
  }
}
