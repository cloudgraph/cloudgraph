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




import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.SAXException;

/**
 * State marshalling related tests.
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public class StateMarshalingTestCase extends StateTestCase {
    private static Log log = LogFactory.getLog(StateMarshalingTestCase.class);
    
    private ValidatingDataBinding binding;
    
    public void setUp() throws Exception {
    }
        
    public void testRawUnmarshal() throws JAXBException, SAXException, FileNotFoundException {
    	log.info("testUnmarshal");
    	this.binding = new StateValidatingDataBinding();
    	File file = new File("./src/test/resources/state-example.xml");
    	FileInputStream stream = new FileInputStream(file);
    	log.info("validate()");
    	StateModel root = (StateModel)this.binding.validate(stream);
    	log.info("marshal()");
    	String xml = this.binding.marshal(root);
    	log.info(xml);
    }
     
    public void testMarshal() throws JAXBException, SAXException, FileNotFoundException {
    	
    	log.info("testUnmarshal");
    	StateMarshalingContext context = new SimpleStateMarshallingContext(
    			new StateNonValidatingDataBinding());
    	SequenceGenerator state = new BindingSequenceGenerator(java.util.UUID.randomUUID(), context);
    	String xml = state.marshal(true);
    	log.info("marshal: " + xml);
    	
    	      
    }  
      
}