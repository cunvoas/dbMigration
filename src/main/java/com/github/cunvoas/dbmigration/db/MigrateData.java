package com.github.cunvoas.dbmigration.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.cunvoas.dbmigration.util.ConnectionConfig;
import com.github.cunvoas.dbmigration.util.ConnectionProvider;
import com.github.cunvoas.dbmigration.util.MigrationException;

public class MigrateData {
	private static final Logger LOGGER = LoggerFactory.getLogger(MigrateData.class);
	
	private Connection sourceConn;
	private Connection destinationConn;
	
	private int batchFlush=50;
	
	/**
	 * Migration d'une base vers une autre.
	 * @param source
	 * @param destination
	 * @throws MigrationException
	 */
	public void migrate(ConnectionConfig source, ConnectionConfig destination) throws MigrationException {
		boolean truncate=true;
		this.setSourceConn(ConnectionProvider.getConnection(source));
		this.setDestinationConn(ConnectionProvider.getConnection(destination));
		
		MetaData metaData = new MetaData();
		metaData.setSource(ConnectionProvider.getConnection(source));
		metaData.setDestination(ConnectionProvider.getConnection(destination));
		
		List<String> srcTableList = metaData.getSourceTables();
		List<String> destTableList = metaData.getDestinationTables();
		
		for (Iterator<String> iterator = srcTableList.iterator(); iterator.hasNext();) {
			String srcTable = iterator.next();
			if (!destTableList.contains(srcTable)) {
				LOGGER.warn("SOURCE TABLE SKIPPED: {}",  srcTable);
				iterator.remove();
			}
		}
		
		for (String table : srcTableList) {
			if (destTableList.contains(table)) {
				this.migrate(table, truncate);
			}
		}
	}
	
	/**
	 * migration d'une liste de table
	 * @param tables
	 * @param truncate
	 * @return
	 * @throws MigrationException
	 */
	public int migrate(List<String> tables, boolean truncate) throws MigrationException {
		int nblig=0;
		try {
			destinationConn.setAutoCommit(false);
			
			int nbTbl=0;
			for (String table : tables) {
				nblig+=migrate(table, truncate);
				LOGGER.warn("TABLE {}/{} ({}%)", ++nbTbl, tables.size(), 100*nbTbl/tables.size());
			}
			
		} catch (SQLException e) {
			LOGGER.error("Migrate TableList", e);
			throw new MigrationException("Migrate TableList", e);
		}
		return nblig;
	}
	
	/**
	 * Migration d'une table
	 * @param table
	 * @param truncate
	 * @return
	 * @throws MigrationException
	 */
	public int migrate(String table, boolean truncate) throws MigrationException {
		
		MetaData metaData = new MetaData();
		metaData.setSource(this.sourceConn);
		metaData.setDestination(this.destinationConn);
		
		int nblig=0;
		
		Statement stSrc=null;
		ResultSet rsSrc = null;
		
		Statement stDst=null;
		PreparedStatement psDst=null;
		
		try {
			if (truncate) {
				// on purge la destination
				stDst = destinationConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				stDst.execute("truncate table "+table);
				stDst.close();
			} else {

				int nbS=-1;
				stSrc = destinationConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				ResultSet rS = stSrc.executeQuery("select count(1) as nb from "+table);
				if (rS.next()) {
					nbS = rS.getInt("nb");
				}
				rS.close();
				stSrc.close();
				
				int nbD=-1;
				stDst = destinationConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				ResultSet rD = stDst.executeQuery("select count(1) as nb from "+table);
				if (rD.next()) {
					nbD = rD.getInt("nb");
				}
				rD.close();
				stDst.close();
				
				if (nbS==nbD && nbS!=0) {
					LOGGER.info("SKIP TABLE {}", table);
					return 0;
				}
			}
			
			stSrc = sourceConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rsSrc = stSrc.executeQuery("select * from "+table);
			
			List<ColumnDescriptor> cols = metaData.getColumns(table);
			StringBuilder sbColsBuilder = new StringBuilder();
			StringBuilder sbValsBuilder = new StringBuilder();
			
			for (ColumnDescriptor columnDescriptor : cols) {
				if (sbColsBuilder.length()>0) {
					sbColsBuilder.append(",");
					sbValsBuilder.append(",");
				}
				sbColsBuilder.append(columnDescriptor.getName());
				sbValsBuilder.append("?");
			}
			
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ");
			sb.append(table);
			sb.append(" (");
			// columns names
			sb.append(sbColsBuilder);
			
			sb.append(") VALUES (");
			sb.append(sbValsBuilder);
			
			sb.append(")");
			LOGGER.info("INSERT QUERY: {}", sb.toString());
			
			psDst = destinationConn.prepareStatement(sb.toString());
			while (rsSrc.next()) {
				
				// maps rs sur ps
				map(psDst, rsSrc, cols);
				
				psDst.addBatch();
				
				nblig++;
				if (nblig%batchFlush==0) {
					psDst.executeBatch();
					psDst.clearBatch();
				}
			}
			
			// on fini les lignes en attente
			if (nblig%batchFlush!=0) {
				psDst.executeBatch();
			}
			destinationConn.commit();
			
			
			
		} catch (SQLException e) {
			LOGGER.error("Table:"+table, e);
			throw new MigrationException("Table:"+table, e);
		}
		
		LOGGER.info("{} rows inserted in '{}' table", nblig, table);
		return nblig;
	}
	
	
	/**
	 * @param ps
	 * @param rs
	 * @throws SQLException
	 */
	private void map(PreparedStatement ps, ResultSet rs, List<ColumnDescriptor> cols) throws SQLException, MigrationException {
		for(int parameterIndex=1; parameterIndex<=cols.size(); parameterIndex++) {
			ColumnDescriptor col = cols.get(parameterIndex-1);
			
			if (col.isNVarcharMax()) {
				LOGGER.debug("ACCESS TO NVARCHAR:", col);
				String content = getVarBinaryAsChar(rs, parameterIndex);
				LOGGER.debug("NVARCHAR CONTENT: {}", content);
				
				ps.setString(parameterIndex, content);
				
//				if (parameterIndex==4) {
//					
//					//InputStream is = IOUtils.toInputStream(content);
////					ps.setBlob(parameterIndex, is);
//					//ps.setNString(parameterIndex, content);
////					ps.setBytes(parameterIndex, content.getBytes());
////					ps.setAsciiStream(parameterIndex, is);
//					
//					Blob blob = this.destinationConn.createBlob();
//					blob.setBytes(1, content.getBytes());
//					ps.setBlob(parameterIndex, blob);
//				}
				
				
			} else {
				Object content = rs.getObject(parameterIndex);
				ps.setObject(parameterIndex, content);
			}
		}
		
		
	}

	/**
	 * Setter for  sourceConn.
	 * @param sourceConn the sourceConn to set
	 */
	public void setSourceConn(Connection sourceConn) throws MigrationException {
		this.sourceConn = sourceConn;
		try {
			this.sourceConn.setReadOnly(true);
		} catch (Exception e) {
			LOGGER.error("MISE EN READ ONLY IMPOSSIBLE", e.getMessage());
			throw new MigrationException("MISE EN READ ONLY IMPOSSIBLE", e);
		}
	}

	/**
	 * Setter for  batchFlush.
	 * @param batchFlush the batchFlush to set
	 */
	public void setBatchFlush(int batchFlush) {
		this.batchFlush = batchFlush;
	}

	/**
	 * Setter for  destinationConn.
	 * @param destinationConn the destinationConn to set
	 */
	public void setDestinationConn(Connection destinationConn) {
		this.destinationConn = destinationConn;
	}
	
	

	/**
	 * Mapping NVARBINARY(MAX).
	 * @param rs
	 * @param colIdx
	 * @return
	 * @throws SQLException
	 * @see https://msdn.microsoft.com/fr-fr/library/ms378813(v=sql.110).aspx
	 */
	private String getVarBinaryAsChar(ResultSet rs, int colIdx) throws SQLException{
		String nvarchar = rs.getNString(colIdx);
		return nvarchar;
	}
	
//	/**
//	 * Mapping NVARBINARY(MAX).
//	 * @param rs
//	 * @param colIdx
//	 * @return
//	 * @throws SQLException
//	 * @throws MigrationException
//	 * @see https://msdn.microsoft.com/fr-fr/library/ms378813(v=sql.110).aspx
//	 */
//	private String getVarBinaryAsChar2(ResultSet rs, int colIdx, ColumnDescriptor col) throws MigrationException{
//		BufferedReader br = null;
//		Reader reader = null;  
//		StringBuilder sb = new StringBuilder();
//		try {
//			String dataString = rs.getString(colIdx);
//			
//			String dataString2 = rs.getNString(colIdx);
//			
//			
//			reader = rs.getCharacterStream(colIdx); 
//			if (reader instanceof BufferedReader) {
//				br = (BufferedReader)reader;
//			} else if (reader!=null) {
//				br = new BufferedReader(reader);
//			} else {
//				return null;
//			}
//			
//			 
//			String sCurrentLine;
//			while ((sCurrentLine = br.readLine()) != null) {
//				sb.append(sCurrentLine);
//			}
//
//		} catch (Exception e) {
//			LOGGER.error("VARBINARY(MAX) "+col.getName(), e);
//			throw new MigrationException("VARBINARY(MAX) "+col.getName(), e);
//		} finally {
//			IOUtils.closeQuietly(br);
//			//IOUtils.closeQuietly(reader);
//		}
//		return sb.toString();
//
//	}
//	
//	
//	/**
//	 * Mapping NVARBINARY(MAX).
//	 * @param rs
//	 * @param colIdx
//	 * @return
//	 * @throws SQLException
//	 * @see https://msdn.microsoft.com/fr-fr/library/ms378813(v=sql.110).aspx
//	 * 
//	 */
//	private String getVarBinaryAsBin(ResultSet rs, int colIdx) throws SQLException{
//		InputStream is = rs.getBinaryStream(colIdx);  
//		return null;
//	}

}
