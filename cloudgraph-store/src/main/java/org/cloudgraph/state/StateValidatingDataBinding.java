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
