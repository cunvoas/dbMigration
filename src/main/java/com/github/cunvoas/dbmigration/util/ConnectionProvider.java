package com.github.cunvoas.dbmigration.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ResourceBundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection provider.
 * @author cunvoas
 */
public class ConnectionProvider {

	private static final ResourceBundle CONFIG = ResourceBundle.getBundle("config");
	private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProvider.class);
	
	/**
	 * Check Integrated Security for SQL Server.
	 * @param config
	 * @return
	 */
	private static boolean isSqlServerIntegratedSecurity(ConnectionConfig config) {
		boolean ret = false;
		
		if ("com.microsoft.sqlserver.jdbc.SQLServerDriver".equals(config.getDriverName())) {
			ret = config.getDatasource().toLowerCase().contains("integratedsecurity=true");
			if (!ret) {
				ret = config.getDatasource().toLowerCase().contains("integratedsecurity=yes");
			}
			if (!ret) {
				ret = config.getDatasource().toLowerCase().contains("integratedsecurity=sspi");
			}
		}
		
		return ret;
	}
	
	
	private static ConnectionConfig getConfig(String location) {
		String database = CONFIG.getString(location+".database");
		String driverName = CONFIG.getString(location+".driverClass");
		String datasource = CONFIG.getString(location+".datasource");
		String user = CONFIG.getString(location+".user");
		String pass = CONFIG.getString(location+".pass");
		
		ConnectionConfig config = new ConnectionConfig();
		config.setDatabase(database);
		config.setDriverName(driverName);
		config.setDatasource(datasource);
		config.setUser(user);
		config.setPass(pass);
		return config;
	}
	
	/**
	 * return JDBC COnnection.
	 * @param config
	 * @return
	 * @throws MigrationException
	 */
	public static final Connection getConnection(ConnectionConfig config) throws MigrationException {
		Connection conn = null;
		try {
			Class.forName(config.getDriverName());
			
			if (isSqlServerIntegratedSecurity(config)) {
				conn = DriverManager.getConnection(config.getDatasource());
			} else {
				conn = DriverManager.getConnection(config.getDatasource(), config.getUser(), config.getPass());
			}
		} catch (Exception e) {
			LOGGER.error("Source CONNECTION", e);
			throw new MigrationException("Source CONNECTION", e);
		}
		return conn;
	}
	
	/**
	 * For test only.
	 * @deprecated
	 * @return source Connection
	 * @throws MigrationException
	 */
	public static final Connection getSourceFromProperties() throws MigrationException {
		ConnectionConfig config = getConfig("source");
		return getConnection(config);
	}
	

	/**
	 * For test only.
	 * @deprecated
	 * @return Destination Connection
	 * @throws MigrationException
	 */
	public static final Connection getDestinationFromProperties() throws MigrationException {
		ConnectionConfig config = getConfig("destination");
		return getConnection(config);
	}
	
	/**
	 * For test only.
	 * @deprecated
	 * @return
	 */
	public static final String getSourceDatabaseFromProperties() {
		return CONFIG.getString("source.database");
	}
	
	/**
	 * For test only.
	 * @deprecated
	 * @return
	 */
	public static final String getDestinationDatabaseFromProperties() {
		return CONFIG.getString("destination.database");
	}
	
	
	

}
