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
package org.cloudgraph.config;

import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.UnmarshalException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.common.bind.BindingValidationEventHandler;
import org.plasma.common.bind.DataBinding;
import org.plasma.common.bind.ValidatingUnmarshaler;
import org.xml.sax.SAXException;

/**
 * Configuration JAXB Binding delegate.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CloudGraphConfigDataBinding implements DataBinding {

	private static Log log = LogFactory
			.getLog(CloudGraphConfigDataBinding.class);
	public static String FILENAME_SCHEMA_CHAIN_ROOT = "cloudgraph-config.xsd";

	// just classes in the same package where can find the above respective
	// schema files via Class.getResource*
	public static Class<?> RESOURCE_CLASS = CloudGraphConfigDataBinding.class;

	private ValidatingUnmarshaler validatingUnmarshaler;

	public static Class<?>[] FACTORIES = {org.cloudgraph.config.ObjectFactory.class,};

	@SuppressWarnings("unused")
	private CloudGraphConfigDataBinding() {
	}

	public CloudGraphConfigDataBinding(
			BindingValidationEventHandler validationEventHandler)
			throws JAXBException, SAXException {
		log.debug("loading schema chain...");
		InputStream stream = RESOURCE_CLASS
				.getResourceAsStream(FILENAME_SCHEMA_CHAIN_ROOT);
		if (stream == null)
			stream = RESOURCE_CLASS.getClassLoader().getResourceAsStream(
					FILENAME_SCHEMA_CHAIN_ROOT);
		if (stream == null)
			throw new CloudGraphConfigurationException(
					"could not find configuration file schema resource '"
							+ FILENAME_SCHEMA_CHAIN_ROOT
							+ "' on the current classpath");
		validatingUnmarshaler = new ValidatingUnmarshaler(stream,
				JAXBContext.newInstance(FACTORIES), validationEventHandler);
	}

	public Class<?>[] getObjectFactories() {
		return FACTORIES;
	}

	public String marshal(Object root) throws JAXBException {
		return validatingUnmarshaler.marshal(root);
	}

	public void marshal(Object root, OutputStream stream) throws JAXBException {
		validatingUnmarshaler.marshal(root, stream);
	}

	public Object unmarshal(String xml) throws JAXBException {
		return validatingUnmarshaler.unmarshal(xml);
	}

	public Object unmarshal(InputStream stream) throws JAXBException {
		return validatingUnmarshaler.unmarshal(stream);
	}

	public Object validate(String xml) throws JAXBException {
		return validatingUnmarshaler.validate(xml);
	}

	public Object validate(InputStream xml) throws JAXBException,
			UnmarshalException {
		return validatingUnmarshaler.validate(xml);
	}

	public BindingValidationEventHandler getValidationEventHandler()
			throws JAXBException {
		return this.validatingUnmarshaler.getValidationEventHandler();
	}

}
