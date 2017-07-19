package org.cloudgraph.store.key;

import java.nio.charset.Charset;

import org.plasma.sdo.core.CoreConstants;


/**
 * Fields used to describe a graph/row.
 * @author Scott Cinnamond
 * @since 1.0.0
 */
public enum GraphMetaField implements MetaField {

	/**
	 * The timestamp for an entire graph indicating the last modified
	 * date for the graph, the value being a string representation of a long integer.  
	 */
	__TS__(),
	/**
	 * The UUID for the data object
	 * which is the data graph root.
	 */
	__RU__(),
	/** 
	 * delimited qualified type name 
	 * for the root data object 
	 */
	__RT__(),
	/**
	 * The sequence mapping.
	 */
	__SMP__(),
	/**
	 * The name of the table column containing the toumbstone of a graph.
	 */
	__TSTN__();	
	
	byte[] bytesName;
	private GraphMetaField() {
		this.bytesName = this.name().getBytes(Charset.forName( CoreConstants.UTF8_ENCODING ));
	}
	
	@Override
    public byte[] asBytes() {
    	return this.bytesName;
    }

	@Override
	public String asString() {
		return this.name();
	}
}
