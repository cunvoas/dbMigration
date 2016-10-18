package com.github.cunvoas.dbmigration.db;

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.github.cunvoas.dbmigration.db.AuditBase;
import com.github.cunvoas.dbmigration.util.ConnectionProvider;

public class TestAuditBase {
	private AuditBase tested = null;
	
	@Before
	public void setUp() throws Exception {
		tested = new AuditBase();
	}

//	@Test
	public void test() {
		try {
			tested.audit(ConnectionProvider.getSource(), ConnectionProvider.getDestination());
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
		
	}

}
