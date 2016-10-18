package com.github.cunvoas.dbmigration.db;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class ColumnDescriptor implements Comparable<ColumnDescriptor> {
	private static final int NVARCHAR_MAX = 1073741823;
	private static final String NVARCHAR_TYPE = "nvarchar";

	private int number;
	private String name;
	private String className;

	// getColumnTypeName nvarchar
	// getPrecision 1073741823
	private String columnTypeName;
	private int precision;

	/**
	 * Getter for name.
	 * 
	 * @return the name
	 */
	public final String getName() {
		return name;
	}

	/**
	 * Setter for name.
	 * 
	 * @param name
	 *            the name to set
	 */
	public final void setName(String name) {
		this.name = name;
	}

	/**
	 * Getter for className.
	 * 
	 * @return the className
	 */
	public final String getClassName() {
		return className;
	}

	/**
	 * Setter for className.
	 * 
	 * @param className
	 *            the className to set
	 */
	public final void setClassName(String className) {
		this.className = className;
	}

	/**
	 * Getter for number.
	 * 
	 * @return the number
	 */
	public final int getNumber() {
		return number;
	}

	/**
	 * Setter for number.
	 * 
	 * @param number
	 *            the number to set
	 */
	public final void setNumber(int number) {
		this.number = number;
	}

	/**
	 * Setter for columnTypeName.
	 * 
	 * @param columnTypeName
	 *            the columnTypeName to set
	 */
	public void setColumnTypeName(String columnTypeName) {
		this.columnTypeName = columnTypeName;
	}

	/**
	 * Setter for precision.
	 * 
	 * @param precision
	 *            the precision to set
	 */
	public void setPrecision(int precision) {
		this.precision = precision;
	}

	/**
	 * @return NVarcharMax
	 */
	public boolean isNVarcharMax() {
		return NVARCHAR_MAX == this.precision
				&& NVARCHAR_TYPE.equals(this.columnTypeName);
	}

	/**
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(ColumnDescriptor other) {
		CompareToBuilder ctb = new CompareToBuilder();
		ctb.append(this.getNumber(), other.getNumber());
		ctb.append(this.getName(), other.getName());
		ctb.append(this.getClassName(), other.getClassName());
		ctb.append(this.precision, other.precision);
		return ctb.toComparison();
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		ToStringBuilder tsb = new ToStringBuilder(this);
		tsb.append("pos", this.number);
		tsb.append("name", this.name);
		tsb.append("type", this.columnTypeName);
		tsb.append("class", this.className);
		return tsb.toString();
	}

	/**
	 * Getter for columnTypeName.
	 * @return the columnTypeName
	 */
	public final String getColumnTypeName() {
		return columnTypeName;
	}

	/**
	 * Getter for precision.
	 * @return the precision
	 */
	public final int getPrecision() {
		return precision;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		boolean ret = false;
		if (obj instanceof ColumnDescriptor) {
			ColumnDescriptor other= (ColumnDescriptor)obj;
			EqualsBuilder eb = new EqualsBuilder();
			eb.append(this.getName(), other.getName());
			ret =  eb.isEquals();
		}
		return ret;
	}

}
