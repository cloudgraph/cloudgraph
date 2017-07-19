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

import javax.xml.bind.JAXBException;

/**
 * A non validating data binding.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public interface NonValidatingDataBinding {

	public abstract String marshal(Object root) throws JAXBException;

	public abstract void marshal(Object root, OutputStream stream)
			throws JAXBException;

	public abstract void marshal(Object root, OutputStream stream,
			boolean formattedOutput) throws JAXBException;

	public abstract Object unmarshal(String xml) throws JAXBException;

	public abstract Object unmarshal(InputStream stream) throws JAXBException;

}