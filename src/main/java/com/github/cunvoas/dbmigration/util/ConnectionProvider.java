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
	
	public static final Connection getConnection(ConnectionConfig config) throws MigrationException {
		Connection conn = null;
		try {
			Class.forName(config.getDriverName());
			conn = DriverManager.getConnection(config.getDatasource(), config.getUser(), config.getPass());
		} catch (Exception e) {
			LOGGER.error("Source CONNECTION", e);
			throw new MigrationException("Source CONNECTION", e);
		}
		return conn;
	}
	
	/**
	 * @return source Connection
	 * @throws MigrationException
	 */
	public static final Connection getSource() throws MigrationException {
		ConnectionConfig config = getConfig("source");
		return getConnection(config);
	}
	

	/**
	 * @return Destination Connection
	 * @throws MigrationException
	 */
	public static final Connection getDestination() throws MigrationException {
		ConnectionConfig config = getConfig("destination");
		return getConnection(config);
	}
	
	public static final String getSourceDatabase() {
		return CONFIG.getString("source.database");
	}
	
	public static final String getDestinationDatabase() {
		return CONFIG.getString("destination.database");
	}
	
	
	

}
