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
package org.cloudgraph.hbase.scan;

import java.io.IOException;
import java.util.Date;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.test.datatypes.Node;
import org.cloudgraph.test.datatypes.query.QStringNode;
import org.plasma.common.test.PlasmaTestSetup;

import commonj.sdo.DataGraph;

/**
 * String SDO datatype specific row-key get operations test. 
 * @author Scott Cinnamond
 * @since 0.5.5
 */

public class StringRowKeyGetTest extends StringScanTest {
    private static Log log = LogFactory.getLog(StringRowKeyGetTest.class);

    public static Test suite() {
        return PlasmaTestSetup.newTestSetup(StringRowKeyGetTest.class);
    }
    
    public void setUp() throws Exception {
        super.setUp();
    } 
    
    public void testEqual() throws IOException       
    {
        long rootId = System.currentTimeMillis();
        long id = rootId + WAIT_TIME;
        Date now = new Date(id);
        Node root = this.createGraph(rootId, id, now, "AAA");
        service.commit(root.getDataGraph(), USERNAME);

        // create 2 more w/same id but new date
        long id2 = id + WAIT_TIME;
        Date now2 = new Date(id2);
        Node root2 = this.createGraph(rootId, id2, now2, "BBB");
        service.commit(root2.getDataGraph(), USERNAME);

        long id3 = id2 + WAIT_TIME;
        Date now3 = new Date(id3);
        Node root3 = this.createGraph(rootId, id3, now3, "CCC");
        service.commit(root3.getDataGraph(), USERNAME);                
        
        // fetch  
        Node fetched = this.fetchSingleGraph(root2.getRootId(), 
        		root2.getStringField());
        String xml = serializeGraph(fetched.getDataGraph());
        log.debug("GRAPH: " + xml);
        assertTrue(fetched.getRootId() == rootId);
        
    }  
     
   
    protected Node fetchSingleGraph(long rootId, String name) {    	
    	QStringNode root = createSelect();
    	
    	root.where(root.rootId().eq(rootId)
        		.and(root.stringField().eq(name)));
    	
    	this.marshal(root.getModel(), rootId);
    	
    	DataGraph[] result = service.find(root);
    	assertTrue(result != null);
    	//FIXME: failing
    	assertTrue("expected 1 results", result.length == 1);
    	
    	return (Node)result[0].getRootObject();
    }

  }
