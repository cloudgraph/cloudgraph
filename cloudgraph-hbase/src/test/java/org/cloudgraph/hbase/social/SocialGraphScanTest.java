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
package org.cloudgraph.hbase.social;

import java.io.IOException;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.test.SocialGraphModelTest;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.plasma.common.test.PlasmaTestSetup;

/**
 * Tests scan operations
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class SocialGraphScanTest extends SocialGraphModelTest {
	private static Log log = LogFactory.getLog(SocialGraphScanTest.class);

	public static Test suite() {
		return PlasmaTestSetup.newTestSetup(SocialGraphScanTest.class);
	}

	public void setUp() throws Exception {
		super.setUp();
	}

	public void testInsert() throws IOException {
		GraphInfo graph1 = createSimpleGraph();
		String xml = this.serializeGraph(graph1.actor.getDataGraph());
		log.debug("inserting graph1:");
		log.debug(xml);
		this.service.commit(graph1.actor.getDataGraph(), "test1");

		GraphInfo graph2 = createSimpleGraph();
		xml = this.serializeGraph(graph2.actor.getDataGraph());
		log.debug("inserting graph2:");
		log.debug(xml);
		this.service.commit(graph2.actor.getDataGraph(), "test2");

		GraphInfo graph3 = createSimpleGraph();
		xml = this.serializeGraph(graph3.actor.getDataGraph());
		log.debug("inserting graph3:");
		log.debug(xml);
		this.service.commit(graph3.actor.getDataGraph(), "test3");

		log.debug("fetching initial graphs");
		Actor[] fetchedActors = fetchGraphs(createTopicScanQuery(
				graph1.actor.getName(), graph2.actor.getName(),
				graph3.actor.getName(), graph1.weather, graph1.politics));
		assertTrue(fetchedActors != null && fetchedActors.length == 3);
		for (Actor actor : fetchedActors) {
			xml = this.serializeGraph(actor.getDataGraph());
			log.debug(xml);
		}

		/*
		 * fetchedActor = fetchGraph(
		 * createTopicWildcardScanQuery(graph1.actor.getName())); xml =
		 * this.serializeGraph(fetchedActor.getDataGraph()); log.debug(xml);
		 */

	}

}
