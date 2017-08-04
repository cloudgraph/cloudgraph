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
