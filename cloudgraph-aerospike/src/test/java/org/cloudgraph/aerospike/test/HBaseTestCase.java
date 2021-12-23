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
package org.cloudgraph.aerospike.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.CommonTest;
import org.cloudgraph.store.StoreServiceContext;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.Query;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.sdo.access.client.HBasePojoDataAccessClient;
import org.plasma.sdo.access.client.SDODataAccessClient;
import org.plasma.sdo.helper.PlasmaXMLHelper;
import org.plasma.sdo.xml.DefaultOptions;
import org.xml.sax.SAXException;

import commonj.sdo.DataGraph;
import commonj.sdo.helper.XMLDocument;

/**
 * @author Scott Cinnamond
 * @since 0.5
 */
public abstract class HBaseTestCase extends CommonTest {
  private static Log log = LogFactory.getLog(HBaseTestCase.class);
  protected SDODataAccessClient service;
  protected String classesDir = System.getProperty("classes.dir");
  protected String targetDir = System.getProperty("target.dir");

  public void setUp() throws Exception {
    service = new SDODataAccessClient(new HBasePojoDataAccessClient(new StoreServiceContext()));
  }

  protected String serializeGraph(DataGraph graph) throws IOException {
    DefaultOptions options = new DefaultOptions(graph.getRootObject().getType().getURI());
    options.setRootNamespacePrefix("test");

    XMLDocument doc = PlasmaXMLHelper.INSTANCE.createDocument(graph.getRootObject(), graph
        .getRootObject().getType().getURI(), null);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PlasmaXMLHelper.INSTANCE.save(doc, os, options);
    os.flush();
    os.close();
    String xml = new String(os.toByteArray());
    return xml;
  }

  protected void debugGraph(DataGraph dataGraph) throws IOException {
    String xml = serializeGraph(dataGraph);
    log.debug("GRAPH: " + xml);
  }

  protected void logGraph(DataGraph dataGraph) throws IOException {
    String xml = serializeGraph(dataGraph);
    log.debug("GRAPH: " + xml);
  }

  protected Query marshal(Query query, float id) {
    return marshal(query, String.valueOf(id));
  }

  protected Query marshal(Query query, String id) {
    PlasmaQueryDataBinding binding;
    try {
      binding = new PlasmaQueryDataBinding(new DefaultValidationEventHandler());
      String xml = binding.marshal(query);
      // log.info("query: " + xml);
      String name = "query-" + id + ".xml";
      File file = new File(new File("./target"), name);
      FileOutputStream fos = new FileOutputStream(file);
      binding.marshal(query, fos);
      FileInputStream fis = new FileInputStream(file);
      Query q2 = (Query) binding.unmarshal(fis);
      return q2;
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    } catch (SAXException e) {
      throw new RuntimeException(e);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected String marshal(Query query) {
    PlasmaQueryDataBinding binding;
    try {
      binding = new PlasmaQueryDataBinding(new DefaultValidationEventHandler());
      String xml = binding.marshal(query);
      return xml;
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    } catch (SAXException e) {
      throw new RuntimeException(e);
    }
  }

  protected void waitForMillis(long time) {
    Object lock = new Object();
    synchronized (lock) {
      try {
        log.info("waiting " + time + " millis...");
        lock.wait(time);
      } catch (InterruptedException e) {
        log.error(e.getMessage(), e);
      }
    }
    log.info("...continue");
  }

}