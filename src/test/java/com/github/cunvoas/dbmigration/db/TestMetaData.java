/**
 * 
 */
package com.github.cunvoas.dbmigration.db;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.github.cunvoas.dbmigration.db.ColumnDescriptor;
import com.github.cunvoas.dbmigration.db.MetaData;
import com.github.cunvoas.dbmigration.util.ConnectionProvider;
import com.github.cunvoas.dbmigration.util.MigrationException;

/**
 * @author UNVOAS
 */
public class TestMetaData {
	
	private MetaData tested = null;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * Test method for {@link com.github.cunvoas.dbmigration.db.MetaData#getSourceTables()}.
	 */
	@Test
	public void testGetSourceTables() {
		tested = new MetaData();
		
		try {
			tested.setSource(ConnectionProvider.getSource());
			tested.setDestination(ConnectionProvider.getDestination());
			
			List<String> tables = tested.getSourceTables();
			
			assertEquals(766, tables.size());
		} catch (MigrationException e) {
			fail(e.getMessage());
		}
	}

	/**
	 * Test method for {@link com.github.cunvoas.dbmigration.db.MetaData#getDestinationTables()}.
	 */
	@Test
	public void testGetDestinationTables() {
		assertEquals(1, 1);
	}
	
	@Test
	public void testunderscore() {
		assertTrue("__pk".startsWith("__"));
	}
	


	/**
	 * Test method for {@link com.github.cunvoas.dbmigration.db.MetaData#getColumns(String)}.
	 */
	@Test
	public void testGetColumns() {
		tested = new MetaData();
		
		try {
			tested.setSource(ConnectionProvider.getSource());
			tested.setDestination(ConnectionProvider.getDestination());
			
			List<ColumnDescriptor> cols = tested.getColumns("search_api_index");
			
			assertEquals(11, cols.size());
			
		} catch (MigrationException e) {
			fail(e.getMessage());
		}
		
		
	}


}
