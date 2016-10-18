package com.github.cunvoas.dbmigration.db;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.cunvoas.dbmigration.util.MigrationException;


/**
 * CSV de résultat d'audit.
 * @author CUNVOAS
 */
public class CsvAudit implements Closeable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CsvAudit.class);
	
	private static CSVFormat format = CSVFormat.DEFAULT.withHeader(CsvAudit.headerCsv()).withDelimiter(';');
	private static String headerCsv() {
		return "Type;Niveau;Table;Colonne;Type Java Source;Type BDD Source;Type Java Destination;Type BDD Destination";
	}
	
	private CSVPrinter csvPrinter = null;
	private PrintStream csvFilePrint = null;
	
	
	public CsvAudit(File auditFile) throws MigrationException {
		super();
		try {
			csvFilePrint = new PrintStream(auditFile);
			csvPrinter = new CSVPrinter(csvFilePrint, format);
		} catch (FileNotFoundException e) {
			LOGGER.error("/tmp/ introuvable", e);
			this.close();
			throw new  MigrationException("/tmp/ introuvable", e);
		} catch (IOException e) {
			LOGGER.error("acces /tmp/ impossible", e);
			this.close();
			throw new  MigrationException("acces /tmp/ impossible", e);
		}
	}
	
	/**
	 * imprime un écart.
	 * @param type
	 * @param niveau
	 * @param table
	 * @param src
	 * @param dst
	 * @throws IOException
	 */
	public void print(String type, String niveau, String table, ColumnDescriptor src, ColumnDescriptor dst)  throws MigrationException {
		try {
			this.print( type, niveau, table, src!=null?src.getName():"", 
					src!=null?src.getClassName():null, src!=null?src.getColumnTypeName():"", 
					dst!=null?dst.getClassName():null, dst!=null?dst.getColumnTypeName():"");
			
		} catch (IOException e) {
			LOGGER.error("CSV PRINT ERROR", e);
			throw new MigrationException("CSV PRINT ERROR", e);
		}
		
	}
	
	/**
	 * imprime une ligne.
	 * @param type
	 * @param niveau
	 * @param table
	 * @param colonne
	 * @param srcTypJava
	 * @param srcTypDbb
	 * @param dstTypJava
	 * @param dstTypDbb
	 * @throws IOException
	 */
	private void print(String type, String niveau, String table, String colonne, String srcTypJava, String srcTypDbb, String dstTypJava, String dstTypDbb) throws IOException {
		this.print(type);
		this.print(niveau);
		this.print(table);
		this.print(colonne);
		this.print(srcTypJava);
		this.print(srcTypDbb);
		this.print(dstTypJava);
		this.print(dstTypDbb);
		csvPrinter.println();
	}
	
	
	/**
	 * iomprime une colonne.
	 * @param string
	 * @throws IOException
	 */
	private void print(String string) throws IOException {
		csvPrinter.print(string != null ? string : "");
		
	}
	
	/**
	 * @see java.io.Closeable#close()
	 */
	public void close() {
		IOUtils.closeQuietly(csvPrinter);
		IOUtils.closeQuietly(csvFilePrint);
	}
}
