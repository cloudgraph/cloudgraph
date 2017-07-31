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

import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventLocator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.common.bind.BindingValidationEventHandler;

/**
 * Configuration parsing (JAXB) event handler.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CloudGraphConfigValidationEventHandler
		implements
			BindingValidationEventHandler {

	private static Log log = LogFactory
			.getLog(CloudGraphConfigValidationEventHandler.class);
	private int errorCount;
	private boolean cumulative = true;

	public int getErrorCount() {
		return errorCount;
	}

	public CloudGraphConfigValidationEventHandler() {
	}
	public CloudGraphConfigValidationEventHandler(boolean cumulative) {
		this.cumulative = cumulative;
	}

	public boolean handleEvent(ValidationEvent ve) {
		boolean result = this.cumulative;
		this.errorCount++;
		ValidationEventLocator vel = ve.getLocator();

		String message = "Line:Col:Offset[" + vel.getLineNumber() + ":"
				+ vel.getColumnNumber() + ":" + String.valueOf(vel.getOffset())
				+ "] - " + ve.getMessage();

		switch (ve.getSeverity()) {
			case ValidationEvent.WARNING :
				log.warn(message);
				break;
			case ValidationEvent.ERROR :
			case ValidationEvent.FATAL_ERROR :
				log.fatal(message);
				throw new CloudGraphConfigurationException(message);
			default :
				log.error(message);
		}
		return result;
	}

	public void reset() {
		this.errorCount = 0;
	}

}
