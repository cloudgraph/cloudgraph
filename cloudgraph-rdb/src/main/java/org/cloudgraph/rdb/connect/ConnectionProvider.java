package org.cloudgraph.rdb.connect;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Supplies connections from a source pool or data source. Connections should always be closed 
 * after use. 
 */
public interface ConnectionProvider {
	
	/**
	 * Returns a connection from an underlying pool or data source. Connections should always be closed 
     * after use. 
	 * @return the connection
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException;
}
