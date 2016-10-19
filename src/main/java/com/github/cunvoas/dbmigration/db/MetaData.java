package com.github.cunvoas.dbmigration.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.cunvoas.dbmigration.config.ExclusionConfig;
import com.github.cunvoas.dbmigration.util.MigrationException;

public class MetaData {
	
	private Connection source = null;
	private Connection destination =  null;
	private String destinationDatabase =  null;
	
	private static final String SQLSERVER_TABLES =  "SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.Tables WHERE TABLE_TYPE = 'BASE TABLE'";
	private static final String MARIADB_TABLES =  "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s'";
	
	private static final String COLUMNS_ENUM = "SELECT * from %s";
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MetaData.class);
	
	
	
	protected List<ColumnDescriptor> getDestinationColumns(String table) throws MigrationException {
		List<ColumnDescriptor> ret = new ArrayList<ColumnDescriptor>();

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		conn = this.destination;
		try {
			
			st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			st.setMaxRows(1);
			rs = st.executeQuery(String.format(COLUMNS_ENUM, table));
			
			ResultSetMetaData rsm = rs.getMetaData();
			int nb = rsm.getColumnCount();
			
			int idxCol=1;
			for (int i=1; i<=nb; i++) {
				
				ColumnDescriptor descriptor = new ColumnDescriptor();
				descriptor.setNumber(idxCol);
				descriptor.setName(rsm.getColumnName(i));
				descriptor.setClassName(rsm.getColumnClassName(i));
				descriptor.setColumnTypeName(rsm.getColumnTypeName(i));
				descriptor.setPrecision(rsm.getPrecision(i));
				ret.add(descriptor);
			}
			
			
		} catch (Exception e) {
			LOGGER.error("Source tables", e);
			throw new MigrationException("Source tables", e);
		}
		
		return ret;
	}
	
	
	public List<ColumnDescriptor> getColumns(String table) throws MigrationException {
		List<ColumnDescriptor> ret = new ArrayList<ColumnDescriptor>();
		
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;

		conn = this.source;
		try {
			st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			st.setMaxRows(1);
			rs = st.executeQuery(String.format(COLUMNS_ENUM, table));
			
			ResultSetMetaData rsm = rs.getMetaData();
			int nb = rsm.getColumnCount();
			
			int idxCol=1;
			for (int i=1; i<=nb; i++) {
				int j=1;
				if (! ExclusionConfig.getINSTANCE().hasExclusion(table, rsm.getColumnName(i))) {

					ColumnDescriptor descriptor = new ColumnDescriptor();
					descriptor.setNumber(idxCol);
					descriptor.setName(rsm.getColumnName(i));
					descriptor.setClassName(rsm.getColumnClassName(i));
					descriptor.setColumnTypeName(rsm.getColumnTypeName(i));
					descriptor.setPrecision(rsm.getPrecision(i));
					ret.add(descriptor);
					
				} else {
					LOGGER.warn("Exlusion applied Table: {} Column: {} Position: {}/{}) ", table, rsm.getColumnName(i), i, nb);
				}

//				//FIXME
//				// colonne technique sqlserver
//				if (rsm.getColumnName(i).startsWith("__")) {
//					LOGGER.warn("Table: {} Champ: {} Colonne: {}/{}) ", table, rsm.getColumnName(i), i, nb);
//					//continue;
//				}
//				
//				//FIXME
//				if ("source_left30".equals(rsm.getColumnName(i))) {
//					LOGGER.warn("Table: {} Champ: {} Colonne: {}/{}) ", table, rsm.getColumnName(i), i, nb);
//					//continue;
//				}
				
			}
			
			
		} catch (Exception e) {
			LOGGER.error("Source tables", e);
			throw new MigrationException("Source tables", e);
		} finally {
//			IOUtils.closeQuietly(rs);
//			IOUtils.closeQuietly(st);
//			IOUtils.closeQuietly(conn);
		}
		
		return ret;
	}
	
	
	/**
	 * @return
	 * @throws MigrationException
	 */
	public List<String> getSourceTables() throws MigrationException {
		List<String> ret = new ArrayList<String>();
		
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = this.source;
			st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = st.executeQuery(String.format(SQLSERVER_TABLES, this.destinationDatabase));
			while(rs.next()) {
				ret.add(rs.getString("TABLE_NAME").toLowerCase());
			}
			
		} catch (Exception e) {
			LOGGER.error("Source tables", e);
			throw new MigrationException("Source tables", e);
		} finally {
//			IOUtils.closeQuietly(rs);
//			IOUtils.closeQuietly(st);
//			IOUtils.closeQuietly(conn);
		}
		
		return ret;
	}
	

	/**
	 * @return
	 * @throws MigrationException
	 */
	public List<String> getDestinationTables() throws MigrationException {
		List<String> ret = new ArrayList<String>();
		
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = this.destination;
			st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = st.executeQuery(String.format(MARIADB_TABLES, this.destinationDatabase));
			while(rs.next()) {
				ret.add(rs.getString("TABLE_NAME").toLowerCase());
			}
		} catch (Exception e) {
			LOGGER.error("Source tables", e);
			throw new MigrationException("Source tables", e);
		} finally {
//			IOUtils.closeQuietly(rs);
//			IOUtils.closeQuietly(st);
//			IOUtils.closeQuietly(conn);
		}
		
		return ret;
	}


	/**
	 * Setter for  source.
	 * @param source the source to set
	 */
	public void setSource(Connection source) {
		this.source = source;
	}


	/**
	 * Setter for  destination.
	 * @param destination the destination to set
	 */
	public void setDestination(Connection destination) {
		this.destination = destination;
	}


	/**
	 * Setter for destinationDatabase.
	 * @param destinationDatabase the destinationDatabase to set
	 */
	public final void setDestinationDatabase(String destinationDatabase) {
		this.destinationDatabase = destinationDatabase;
	}
}
