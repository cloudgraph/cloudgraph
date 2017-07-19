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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.plasma.common.bind.NonValidatingUnmarshaler;
import org.xml.sax.SAXException;

/**
 * State JAXB concurrent non-validating Binding delegate which simply
 * wraps the underlying delegate methods in synchronized blocks.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public class ConcurrentNonValidatingDataBinding implements NonValidatingDataBinding {

	private NonValidatingUnmarshaler unmarshaler;

	public static Class<?>[] FACTORIES = { org.cloudgraph.state.ObjectFactory.class, };

	public ConcurrentNonValidatingDataBinding() throws JAXBException,
			SAXException {
		this.unmarshaler = new NonValidatingUnmarshaler(
				JAXBContext.newInstance(FACTORIES));
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
	public void marshal(Object root, OutputStream stream,
			boolean formattedOutput) throws JAXBException {
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
