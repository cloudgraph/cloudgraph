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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.common.bind.ValidatingUnmarshaler;
import org.xml.sax.SAXException;

/**
 * State JAXB Binding delegate. It is crucial that this binding be cached by
 * service implementations at the appropriate level to 1.) guarantee thread
 * safety (this class is NOT thread safe) and 2.) re-use the underlying JAXB
 * context and parsed schema instance(s) across as many requests as possible.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public class StateValidatingDataBinding implements ValidatingDataBinding {

  private static Log log = LogFactory.getLog(StateValidatingDataBinding.class);
  public static String FILENAME_SCHEMA_CHAIN_ROOT = "cloudgraph-state.xsd";

  public static Class<?> RESOURCE_CLASS = StateValidatingDataBinding.class;

  private ValidatingUnmarshaler unmarshaler;

  public static Class<?>[] FACTORIES = { org.cloudgraph.state.ObjectFactory.class, };

  public StateValidatingDataBinding() throws JAXBException, SAXException {
    log.info("loading schema chain...(note: this is expensive - cache this binding where possible)");
    InputStream stream = RESOURCE_CLASS.getResourceAsStream(FILENAME_SCHEMA_CHAIN_ROOT);
    if (stream == null)
      stream = RESOURCE_CLASS.getClassLoader().getResourceAsStream(FILENAME_SCHEMA_CHAIN_ROOT);
    if (stream == null)
      throw new StateException("could not find configuration file schema resource '"
          + FILENAME_SCHEMA_CHAIN_ROOT + "' on the current classpath");
    this.unmarshaler = new ValidatingUnmarshaler(stream, JAXBContext.newInstance(FACTORIES),
        new StateValidationEventHandler());
  }

  public Class<?>[] getObjectFactories() {
    return FACTORIES;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.ValidatingDataBinding#marshal(java.lang.Object)
   */
  @Override
  public String marshal(Object root) throws JAXBException {

    return unmarshaler.marshal(root);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.ValidatingDataBinding#marshal(java.lang.Object,
   * java.io.OutputStream)
   */
  @Override
  public void marshal(Object root, OutputStream stream) throws JAXBException {
    unmarshaler.marshal(root, stream);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.ValidatingDataBinding#marshal(java.lang.Object,
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
   * @see org.cloudgraph.state.ValidatingDataBinding#validate(java.lang.String)
   */
  @Override
  public Object validate(String xml) throws JAXBException {
    return unmarshaler.validate(xml);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.state.ValidatingDataBinding#validate(java.io.InputStream)
   */
  @Override
  public Object validate(InputStream stream) throws JAXBException {
    return unmarshaler.validate(stream);
  }
}
