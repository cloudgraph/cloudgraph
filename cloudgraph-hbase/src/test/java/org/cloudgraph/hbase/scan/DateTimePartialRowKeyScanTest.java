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
import org.cloudgraph.hbase.test.DataTypeGraphModelTest;
import org.cloudgraph.test.datatypes.DateTimeNode;
import org.cloudgraph.test.datatypes.Node;
import org.cloudgraph.test.datatypes.query.QDateTimeNode;
import org.plasma.common.test.PlasmaTestSetup;
import org.plasma.query.Expression;
import org.plasma.sdo.helper.PlasmaDataFactory;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.DataGraph;
import commonj.sdo.Type;

/**
 * DateTime SDO datatype specific partial row-key scan operations test. 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class DateTimePartialRowKeyScanTest extends DataTypeGraphModelTest {
    private static Log log = LogFactory.getLog(DateTimePartialRowKeyScanTest.class);
    private long WAIT_TIME = 10; 
    private String USERNAME = "datetime_test";
    
    
    public static Test suite() {
        return PlasmaTestSetup.newTestSetup(DateTimePartialRowKeyScanTest.class);
    }
    
    public void setUp() throws Exception {
        super.setUp();
    } 
    
    public void testEqual() throws IOException       
    {
        long rootId = System.currentTimeMillis();

        long id1 = rootId + WAIT_TIME; 
        Date now1 = new Date(id1);
        Node root1 = this.createGraph(rootId, id1, now1);
        service.commit(root1.getDataGraph(), USERNAME);

        long id2 = id1 + WAIT_TIME; 
        Date now2 = new Date(id2);
        Node root2 = this.createGraph(rootId, id2, now2);
        service.commit(root2.getDataGraph(), USERNAME);

        long id3 = id2 + WAIT_TIME; 
        Date now3 = new Date(id3);
        Node root3 = this.createGraph(rootId, id3, now3);
        service.commit(root3.getDataGraph(), USERNAME);        
        
        // fetch a slice
        Node fetched = this.fetchSingleGraph(rootId, id2, 
        		root2.getChild(3).getName(), root2.getDateTimeField());
        debugGraph(fetched.getDataGraph());
    }  
    
    public void testBetween() throws IOException       
    {
        long rootId = System.currentTimeMillis();

        long id1 = rootId + WAIT_TIME; 
        Date now1 = new Date(id1);
        Node root1 = this.createGraph(rootId, id1, now1);
        service.commit(root1.getDataGraph(), USERNAME);

        long id2 = id1 + WAIT_TIME; 
        Date now2 = new Date(id2);
        Node root2 = this.createGraph(rootId, id2, now2);
        service.commit(root2.getDataGraph(), USERNAME);

        long id3 = id2 + WAIT_TIME; 
        Date now3 = new Date(id3);
        Node root3 = this.createGraph(rootId, id3, now3);
        service.commit(root3.getDataGraph(), USERNAME);        
        
        Node[] fetched = this.fetchGraphsBetween(rootId,
        		id1, id3,  
        		root1.getDateTimeField(), root3.getDateTimeField());
        assertTrue(fetched.length == 3);

        //assertTrue(fetchedProfiles[0].getProfileId() == id1);
        debugGraph(fetched[0].getDataGraph());
        debugGraph(fetched[1].getDataGraph());
        debugGraph(fetched[2].getDataGraph());
    } 
     
    public void testInclusive() throws IOException       
    {
        long rootId = System.currentTimeMillis();

        long id1 = rootId + WAIT_TIME; 
        Date now1 = new Date(id1);
        Node root1 = this.createGraph(rootId, id1, now1);
        service.commit(root1.getDataGraph(), USERNAME);

        long id2 = id1 + WAIT_TIME; 
        Date now2 = new Date(id2);
        Node root2 = this.createGraph(rootId, id2, now2);
        service.commit(root2.getDataGraph(), USERNAME);

        long id3 = id2 + WAIT_TIME; 
        Date now3 = new Date(id3);
        Node root3 = this.createGraph(rootId, id3, now3);
        service.commit(root3.getDataGraph(), USERNAME);        
        
        Node[] fetched = this.fetchGraphsInclusive(rootId,
        		id1, id3,  
        		root1.getDateTimeField(), root3.getDateTimeField());
        assertTrue(fetched.length == 3);

        debugGraph(fetched[0].getDataGraph());
        debugGraph(fetched[1].getDataGraph());
        debugGraph(fetched[2].getDataGraph());
    }  
    
    public void testExclusive() throws IOException       
    {
        long rootId = System.currentTimeMillis();

        long id1 = rootId + WAIT_TIME; 
        Date now1 = new Date(id1);
        Node root1 = this.createGraph(rootId, id1, now1);
        service.commit(root1.getDataGraph(), USERNAME);

        long id2 = id1 + WAIT_TIME; 
        Date now2 = new Date(id2);
        Node root2 = this.createGraph(rootId, id2, now2);
        service.commit(root2.getDataGraph(), USERNAME);

        long id3 = id2 + WAIT_TIME; 
        Date now3 = new Date(id3);
        Node root3 = this.createGraph(rootId, id3, now3);
        service.commit(root3.getDataGraph(), USERNAME);        
        
        Node[] fetched = this.fetchGraphsExclusive(
        		id1, id3,  
        		root1.getDateTimeField(), root3.getDateTimeField());
        assertTrue(fetched.length == 1);

        debugGraph(fetched[0].getDataGraph());
    }    
 
    protected Node fetchSingleGraph(long rootId, long id, String name, Object date) {    	
    	QDateTimeNode root = createSelect(name);
    	
    	root.where(root.rootId().eq(rootId).and(
    			root.dateTimeField().eq(date)));
    	
    	DataGraph[] result = service.find(root);
    	assertTrue(result != null);
    	assertTrue(result.length == 1);
    	
    	return (Node)result[0].getRootObject();
    }

    protected Node[] fetchGraphsBetween(long rootId, long min, long max,
    		Object minDate, Object maxDate) {    	
    	QDateTimeNode root = createSelect();
    	root.where(root.rootId().eq(rootId).and(root.dateTimeField().between(minDate, maxDate)));
    	
    	DataGraph[] result = service.find(root);
    	assertTrue(result != null);
    	
    	Node[] profiles = new Node[result.length];
    	for (int i = 0; i < result.length; i++) 
    		profiles[i] = (Node)result[i].getRootObject();
    	return profiles;
    }

    protected Node[] fetchGraphsInclusive(long rootId, long min, long max,
    		Object minDate, Object maxDate) {    	
    	QDateTimeNode root = createSelect();
    	root.where(root.rootId().eq(rootId).and(root.dateTimeField().ge(minDate)
        		.and(root.dateTimeField().le(maxDate))));
    	
    	DataGraph[] result = service.find(root);
    	assertTrue(result != null);
    	
    	Node[] profiles = new Node[result.length];
    	for (int i = 0; i < result.length; i++) 
    		profiles[i] = (Node)result[i].getRootObject();
    	return profiles;
    }
    
    protected Node[] fetchGraphsExclusive(long min, long max,
    		Object minDate, Object maxDate) {    	
    	QDateTimeNode root = createSelect();
    	root.where(root.dateTimeField().gt(minDate)
        		.and(root.dateTimeField().lt(maxDate)));
    	DataGraph[] result = service.find(root);
    	assertTrue(result != null);
    	
    	Node[] profiles = new Node[result.length];
    	for (int i = 0; i < result.length; i++) 
    		profiles[i] = (Node)result[i].getRootObject();
    	return profiles;
    }
    
    private QDateTimeNode createSelect(String name)
    {
    	QDateTimeNode root = QDateTimeNode.newQuery();
    	Expression predicate = root.name().eq(name);
    	root.select(root.wildcard());
    	root.select(root.child(predicate).wildcard());
    	root.select(root.child(predicate).child().wildcard());
    	root.select(root.child(predicate).child().child().wildcard());
    	root.select(root.child(predicate).child().child().child().wildcard());
    	return root;
    }
    
    private QDateTimeNode createSelect()
    {
    	QDateTimeNode root = QDateTimeNode.newQuery();
    	root.select(root.wildcard());
    	root.select(root.child().wildcard());
    	root.select(root.child().child().wildcard());
    	root.select(root.child().child().child().wildcard());
    	root.select(root.child().child().child().child().wildcard());
    	return root;
    }
    
    protected DateTimeNode createGraph(long rootId, long id, Date now) {
        DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
        dataGraph.getChangeSummary().beginLogging(); // log changes from this point
    	Type rootType = PlasmaTypeHelper.INSTANCE.getType(DateTimeNode.class);
    	DateTimeNode root = (DateTimeNode)dataGraph.createRootObject(rootType);
    	fillNode(root, rootId, id, now, "datetime", 0, 0);
    	fillGraph(root, id, now, "datetime");
        return root;
    }
    
}
