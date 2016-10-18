package com.github.cunvoas.dbmigration.util;

/**
 * @author cunvoas
 *
 */
public class MigrationException extends Exception {

	/** serialVersionUID */
	private static final long serialVersionUID = -9178349322604439810L;
	private String msg;
	
	/**
	 * 
	 * @param code
	 * @param e
	 */
	public MigrationException (String msg, Exception e) {
		super(e);
		this.msg=msg;
	}
	
	
	/**
	 * @see java.lang.Throwable#getMessage()
	 */
	@Override
	public String getMessage() {
		return this.msg;
	}


}
