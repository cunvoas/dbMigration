package com.github.cunvoas.dbmigration.config;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author UNVOAS
 */
public class MappingExclusion implements Comparable<MappingExclusion> {
	private String table;
	private String column;
	

	/**
	 * Getter for table.
	 * @return the table
	 */
	public final String getTable() {
		return table;
	}



	/**
	 * Setter for table.
	 * @param table the table to set
	 */
	public final void setTable(String table) {
		this.table = table;
	}



	/**
	 * Getter for column.
	 * @return the column
	 */
	public final String getColumn() {
		return column;
	}

	/**
	 * Setter for column.
	 * @param column the column to set
	 */
	public final void setColumn(String column) {
		this.column = column;
	}

	/**
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(MappingExclusion o) {
		CompareToBuilder ctb = new CompareToBuilder();
		ctb.append(this.getTable(), o.getTable());
		ctb.append(this.getColumn(), o.getColumn());
		return ctb.toComparison();
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		HashCodeBuilder hcb = new HashCodeBuilder();
		hcb.append(this.getTable());
		hcb.append(this.getColumn());
		return hcb.toHashCode();
	}



	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof MappingExclusion) {
			EqualsBuilder eb = new EqualsBuilder();
			eb.append(this.getTable(), ((MappingExclusion) obj).getTable());
			eb.append(this.getColumn(), ((MappingExclusion) obj).getColumn());
			return eb.isEquals();
		} else {
			return false;
		}
	}
}
