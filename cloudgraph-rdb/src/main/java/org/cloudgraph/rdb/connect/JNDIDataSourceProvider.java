package org.cloudgraph.rdb.connect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.config.ConfigurationConstants;
import org.plasma.config.DataAccessProviderName;
import org.plasma.config.PlasmaConfig;
import org.plasma.config.Property;
import org.plasma.sdo.access.DataAccessException;

/**
 * Supplies connections using a JNDI registered datasource.
 */
public class JNDIDataSourceProvider implements DataSourceProvder {
	private static final Log log = LogFactory
			.getLog(JNDIDataSourceProvider.class);
	protected DataSource datasource;

	public JNDIDataSourceProvider() {
		Properties props = new Properties();
		for (Property property : PlasmaConfig.getInstance()
				.getDataAccessProvider(DataAccessProviderName.JDBC)
				.getProperties()) {
			props.put(property.getName(), property.getValue());
		}

		String datasourceName = props
				.getProperty(ConfigurationConstants.JDBC_DATASOURCE_NAME);
		if (datasourceName == null)
			throw new DataAccessException(
					"cannot lookup datasource - datasource name property '"
							+ ConfigurationConstants.JDBC_DATASOURCE_NAME
							+ "' not found in configuration for "
							+ "data access provider '"
							+ DataAccessProviderName.JDBC.name()
							+ "' - a fully qualified JNDI name is required");
		try {
			Context initialContext = new InitialContext();
			this.datasource = (DataSource) initialContext
					.lookup(datasourceName);
			if (this.datasource == null) {
				throw new DataAccessException("cannot lookup datasource '"
						+ datasourceName + "'");
			}
		} catch (NamingException ex) {
			log.error("cannot lookup datasource '" + datasourceName + "'", ex);
			throw new DataAccessException(ex);
		}
	}

	@Override
	public Connection getConnection() throws SQLException {
		return this.datasource.getConnection();
	}

}
