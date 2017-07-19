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
package org.cloudgraph.hbase.test;




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
        service = new SDODataAccessClient(
        		new HBasePojoDataAccessClient());
    }
        
    protected String serializeGraph(DataGraph graph) throws IOException
    {
        DefaultOptions options = new DefaultOptions(
        		graph.getRootObject().getType().getURI());
        options.setRootNamespacePrefix("test");
        
        XMLDocument doc = PlasmaXMLHelper.INSTANCE.createDocument(graph.getRootObject(), 
        		graph.getRootObject().getType().getURI(), 
        		null);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
	    PlasmaXMLHelper.INSTANCE.save(doc, os, options);        
        os.flush();
        os.close(); 
        String xml = new String(os.toByteArray());
        return xml;
    }
    
    protected void debugGraph(DataGraph dataGraph) throws IOException 
    {
        String xml = serializeGraph(dataGraph);
        log.debug("GRAPH: " + xml);    	
    }
    
    protected void logGraph(DataGraph dataGraph) throws IOException 
    {
        String xml = serializeGraph(dataGraph);
        log.debug("GRAPH: " + xml);    	
    }
    
    protected Query marshal(Query query, float id) {
    	return marshal(query, String.valueOf(id));
    }   
    
    protected Query marshal(Query query, String id) {
        PlasmaQueryDataBinding binding;
		try {
			binding = new PlasmaQueryDataBinding(
			        new DefaultValidationEventHandler());
			String xml = binding.marshal(query);
	        //log.info("query: " + xml);
			String name = "query-" + id+ ".xml";
			File file = new File(new File("./target"), name);
			FileOutputStream fos = new FileOutputStream(file);
			binding.marshal(query, fos);
	        FileInputStream fis = new FileInputStream(file);
	        Query q2 = (Query)binding.unmarshal(fis);
	    	return q2;
		} catch (JAXBException e) {
			throw new RuntimeException(e);
		} catch (SAXException e) {
			throw new RuntimeException(e);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
    }
    
    protected void waitForMillis(long time) {
       	Object lock = new Object();
        synchronized (lock) {
	        try {
	        	log.info("waiting "+time+" millis...");
	        	lock.wait(time);
	        }
	        catch (InterruptedException e) {
	        	log.error(e.getMessage(), e);
	        }
        }
        log.info("...continue");
    }
    
}