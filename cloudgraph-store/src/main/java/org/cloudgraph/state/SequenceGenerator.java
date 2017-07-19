package org.cloudgraph.state;

import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataObject;

public interface SequenceGenerator {

	public abstract java.util.UUID getRootUUID();

	public abstract void close();

	public abstract boolean hasLastSequence(PlasmaType type);

	public abstract Long lastSequence(PlasmaType type);

	/**
	 * Creates and adds a sequence number mapped to the UUID within the
	 * given data object.  
	 * @param dataObject the data object
	 * @return the new sequence number
	 * @throws IllegalArgumentException if the data object is already mapped
	 */
	public abstract Long nextSequence(DataObject dataObject);

	public abstract Long nextSequence(PlasmaType type);

	public abstract String marshal();

	String marshal(boolean formatted);

}