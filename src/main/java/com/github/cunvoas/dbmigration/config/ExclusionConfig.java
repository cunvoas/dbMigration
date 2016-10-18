/**
 * 
 */
package com.github.cunvoas.dbmigration.config;

import java.util.List;

/**
 * @author cunvoas
 *
 */
public class ExclusionConfig {

	private static final ExclusionConfig INSTANCE = new ExclusionConfig();
	private List<MappingExclusion> exclusions=null;
	
	private ExclusionConfig() {
		super();
	}
	
	/**
	 * Getter for  INSTANCE.
	 * @return the INSTANCE
	 */
	public static ExclusionConfig getINSTANCE() {
		return INSTANCE;
	}
	
	
	public void setExclusion(List<MappingExclusion> exclusions) {
		this.exclusions = exclusions;
	}
	
	/**
	 * check is table.column is exluded.
	 * @param table
	 * @param column
	 * @return
	 */
	public boolean hasExclusion(String table, String column) {
		if (this.exclusions ==null || table==null || column==null) {
			return false;
		}
		
		String exclusion = String.format("%s.%s", table.toLowerCase(), column.toLowerCase());
		return this.exclusions.contains(exclusion);
	}
	

}
