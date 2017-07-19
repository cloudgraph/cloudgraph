package org.cloudgraph.hbase.io;

import java.util.List;

import org.plasma.sdo.PlasmaType;

public interface EdgeOperation {

	/**
	 * Returns the base type of the collection. Individual collection members
	 * by contain instances of subclasses of the base type. 
	 * @return the base type of the collection.
	 */
	public abstract PlasmaType getBaseType();

	/**
	 * Returns the traversal path (in XPath syntax) for the location of
	 * the collection relative to the root of the graph where the collection is
	 * instantiated. 
	 * @return the traversal path (in XPath syntax) for the location of
	 * the collection relative to the root of the graph where the collection is
	 * instantiated.
	 */
	public abstract String getPath();

	/**
	 * Returns the sequence identifiers for entities of the collection. Only used for local 
	 * (non external) collections. 
	 * @return the sequence identifiers for entities of the collection.
	 */
	public abstract List<Long> getSequences();
	
	/**
	 * Returns the subtype or 
	 * null if none exists
	 * @param sequence the sequence
	 * @return the subtype or 
	 * null if none exists
	 */
	public abstract PlasmaType getSubType();

	/**
	 * Returns the name of the table where the collection is instantiated. Only used for external 
	 * (non local) collections. 
	 * @return the name of the table where the collection is instantiated.
	 */
	public abstract String getTable();

	
	/**
	 * Returns the row keys for entities of the collection. Only used for external 
	 * (non local) collections. 
	 * @return the row keys for entities of the collection.
	 */
	public abstract List<String> getRowKeys();

	/**
	 * Returns true if the collection is external, that is if
	 * the type for the collection is 'bound' to a physical table. 
	 * @return true if the collection is external.
	 */
	public abstract boolean isExternal();

	/**
	 * Returns the (cached) count of members of the collection. Useful where
	 * only a count is needed.  
	 * @return the cached count of members of the collection.
	 */
	public abstract long getCount();



}