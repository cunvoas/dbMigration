package com.github.cunvoas.dbmigration.db;

import static org.junit.Assert.*;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.github.cunvoas.dbmigration.db.MetaData;
import com.github.cunvoas.dbmigration.db.MigrateData;
import com.github.cunvoas.dbmigration.util.ConnectionProvider;
import com.github.cunvoas.dbmigration.util.MigrationException;

public class TestMigrateData {
	private MigrateData tested=null;
	
	@Before
	public void setUp() throws Exception {
		
	}

	@Test
	public void testMigrateListOfString() {
		boolean truncate=true;
		tested = new MigrateData();
		try {
			tested.setSourceConn(ConnectionProvider.getSource());
			tested.setDestinationConn(ConnectionProvider.getDestination());
			
			MetaData metaData = new MetaData();
			metaData.setSource(ConnectionProvider.getSource());
			metaData.setDestination(ConnectionProvider.getDestination());
			
			List<String> tablesList = metaData.getSourceTables();
			
			
			boolean skip=true;
			skip=false;
			if (skip) {
				for (Iterator<String> iterator = tablesList.iterator(); iterator.hasNext();) {
					String string = iterator.next();
	
					if ("locales_source".equals(string)) {
						skip=false;
					}
					
					if (skip) {
						iterator.remove();
					}
				}
			}
			/** TEST **/
			
			List<String> destTables = metaData.getDestinationTables();
			
			for (String table : tablesList) {
				if (!destTables.contains(table)) {
					System.err.println("TABLE SKIPPED: "+ table);
				}
			}
			
			for (String table : tablesList) {
				if (destTables.contains(table)) {
					tested.migrate(table, truncate);
				}
			}
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	//@Test
	public void testMigrateOneTable() {
		tested = new MigrateData();
		try {
			tested.setSourceConn(ConnectionProvider.getSource());
			tested.setDestinationConn(ConnectionProvider.getDestination());
			tested.migrate("locales_source", true);
			
			Connection connection = ConnectionProvider.getDestination();
			Statement s = connection.createStatement();
			ResultSet rSet = s.executeQuery("SELECT * FROM locales_source ");
			if (rSet.next()) {
				Blob blob = rSet.getBlob("source");
				byte[] bdata = blob.getBytes(1, (int) blob.length());
				String s2 = new String(bdata);
				System.out.println(s2);
			}
			
			
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
		
	}

}
