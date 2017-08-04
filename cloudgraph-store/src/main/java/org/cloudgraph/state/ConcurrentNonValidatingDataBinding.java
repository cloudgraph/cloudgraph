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
 * State JAXB concurrent non-validating Binding delegate which simply wraps the
 * underlying delegate methods in synchronized blocks.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public class ConcurrentNonValidatingDataBinding implements NonValidatingDataBinding {

  private NonValidatingUnmarshaler unmarshaler;

  public static Class<?>[] FACTORIES = { org.cloudgraph.state.ObjectFactory.class, };

  public ConcurrentNonValidatingDataBinding() throws JAXBException, SAXException {
    this.unmarshaler = new NonValidatingUnmarshaler(JAXBContext.newInstance(FACTORIES));
  }

  public Class<?>[] getObjectFactories() {
    return FACTORIES;
  }

  @Override
  public String marshal(Object root) throws JAXBException {
    synchronized (this) {
      return unmarshaler.marshal(root);
    }
  }

  @Override
  public void marshal(Object root, OutputStream stream) throws JAXBException {
    synchronized (this) {
      unmarshaler.marshal(root, stream);
    }
  }

  @Override
  public void marshal(Object root, OutputStream stream, boolean formattedOutput)
      throws JAXBException {
    synchronized (this) {
      unmarshaler.marshal(root, stream, formattedOutput);
    }
  }

  @Override
  public Object unmarshal(String xml) throws JAXBException {
    synchronized (this) {
      return unmarshaler.unmarshal(xml);
    }
  }

  @Override
  public Object unmarshal(InputStream stream) throws JAXBException {
    synchronized (this) {
      return unmarshaler.unmarshal(stream);
    }
  }
}
