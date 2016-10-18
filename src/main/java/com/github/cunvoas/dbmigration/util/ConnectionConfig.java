package com.github.cunvoas.dbmigration.util;

public class ConnectionConfig {
	private String database;
	private String driverName;
	private String datasource;
	private String user;
	private String pass;
	
	
	/**
	 * Getter for database.
	 * @return the database
	 */
	public final String getDatabase() {
		return database;
	}
	/**
	 * Setter for database.
	 * @param database the database to set
	 */
	public final void setDatabase(String database) {
		this.database = database;
	}
	/**
	 * Getter for driverName.
	 * @return the driverName
	 */
	public final String getDriverName() {
		return driverName;
	}
	/**
	 * Setter for driverName.
	 * @param driverName the driverName to set
	 */
	public final void setDriverName(String driverName) {
		this.driverName = driverName;
	}
	/**
	 * Getter for datasource.
	 * @return the datasource
	 */
	public final String getDatasource() {
		return datasource;
	}
	/**
	 * Setter for datasource.
	 * @param datasource the datasource to set
	 */
	public final void setDatasource(String datasource) {
		this.datasource = datasource;
	}
	/**
	 * Getter for user.
	 * @return the user
	 */
	public final String getUser() {
		return user;
	}
	/**
	 * Setter for user.
	 * @param user the user to set
	 */
	public final void setUser(String user) {
		this.user = user;
	}
	/**
	 * Getter for pass.
	 * @return the pass
	 */
	public final String getPass() {
		return pass;
	}
	/**
	 * Setter for pass.
	 * @param pass the pass to set
	 */
	public final void setPass(String pass) {
		this.pass = pass;
	}

}
