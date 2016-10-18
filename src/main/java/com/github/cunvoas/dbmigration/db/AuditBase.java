package com.github.cunvoas.dbmigration.db;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.cunvoas.dbmigration.util.MigrationException;

public class AuditBase {
	private static final Logger LOGGER = LoggerFactory.getLogger(AuditBase.class);
	
	public void audit(Connection source, Connection destination) throws MigrationException {
		
		CsvAudit csv=new CsvAudit(new File("/tmp/audit-migration.csv"));
		
		MetaData metaData = new MetaData();
		metaData.setSource(source);
		metaData.setDestination(destination);
		
		List<String> srcTables = metaData.getSourceTables();
		List<String> dstTables = metaData.getDestinationTables();
		
		// analyse source vs destination
		for (String table : srcTables) {
			if (!dstTables.contains(table)) {
				LOGGER.error("TABLE PRESENT IN SOURCE ONLY: {}", table);
				csv.print("TABLE PRESENT IN SOURCE ONLY ", "ERROR", table, null, null);
			}
		}

		// analyse destination vs source
		for (String table : dstTables) {
			if (!srcTables.contains(table)) {
				LOGGER.error("TABLE PRESENT IN DESTINATION ONLY: {}", table);
				csv.print("TABLE PRESENT IN DESTINATION ONLY", "WARN", table, null, null);
			}
		}
		
		// tables communes
		for (Iterator<String> iterator = srcTables.iterator(); iterator.hasNext();) {
			String table = (String) iterator.next();
			if (!dstTables.contains(table)) {
				iterator.remove();
			}
		}
		
		for (Iterator<String> iterator = dstTables.iterator(); iterator.hasNext();) {
			String table = (String) iterator.next();
			if (!srcTables.contains(table)) {
				iterator.remove();
			}
		}

		// analyse des colonnes
		Map<String, List<ColumnDescriptor>> mapSrcCols= new HashMap<String, List<ColumnDescriptor>>();
		Map<String, List<ColumnDescriptor>> mapDstCols= new HashMap<String, List<ColumnDescriptor>>();
		for (String table : srcTables) {
			List<ColumnDescriptor> scols = metaData.getColumns(table);
			Collections.sort(scols, ColumnDescriptorComparator.INSTANCE);
			mapSrcCols.put(table, scols);
			
			List<ColumnDescriptor> dcols = metaData.getDestinationColumns(table);
			Collections.sort(dcols, ColumnDescriptorComparator.INSTANCE);
			mapDstCols.put(table, dcols);
		}
		
		
		
		// comparaison
		for (Iterator<Entry<String, List<ColumnDescriptor>>> iterator = mapSrcCols.entrySet().iterator(); iterator.hasNext();) {
			Entry<String, List<ColumnDescriptor>> entry = iterator.next();
			
			String table = entry.getKey();
			List<ColumnDescriptor> srcCols= entry.getValue();
			List<ColumnDescriptor> dstCols=mapDstCols.get(table);
			
			if (srcCols.size()!=dstCols.size()) {
				LOGGER.error("TABLE SOURCE & DESTINATION HAVE DIFFERENT NUMBER OF COLUMNS: {}", table);
				csv.print("TABLE SOURCE & DESTINATION HAVE DIFFERENT NUMBER OF COLUMNS: ", "WARN", table, null, null);
				
				List<ColumnDescriptor> biggest = new ArrayList<ColumnDescriptor>();
				List<ColumnDescriptor> smallest = new ArrayList<ColumnDescriptor>();
				String smallDesc="";
				String bigDesc="";
				
				if (srcCols.size()>dstCols.size()) {
					biggest.addAll(srcCols);
					bigDesc="source";
					smallest.addAll(dstCols);
					smallDesc="destination";
				} else {
					smallest.addAll(srcCols);
					smallDesc="source";
					biggest.addAll(dstCols);
					bigDesc="destination";
				}
				
				for (Iterator<ColumnDescriptor> biggestIter = biggest.iterator(); biggestIter.hasNext();) {
					ColumnDescriptor column = biggestIter.next();
					if (smallest.contains(column)) {
						biggestIter.remove();
						smallest.remove(column);
					}
				}
				

				for (ColumnDescriptor col : biggest) {
					LOGGER.error("COLUMNS {} ({}) pos={} ONLY PRESENT IN {}", col.getName(), col.getColumnTypeName(), col.getNumber(), bigDesc);
					
					if("destination".equals(bigDesc)) {
						csv.print("COLUMNS ONLY IN", "ERROR", table, null, col);
					} else {
						csv.print("COLUMNS ONLY IN", "ERROR", table,col, null);
					}
				}
				for (ColumnDescriptor col : smallest) {
					LOGGER.error("COLUMNS {} ({}) pos={} ONLY PRESENT IN {}", col.getName(), col.getColumnTypeName(), col.getNumber(), smallDesc);
					
					if("destination".equals(smallDesc)) {
						csv.print("COLUMNS ONLY IN", "ERROR", table, null, col);
					} else {
						csv.print("COLUMNS ONLY IN", "ERROR", table,col, null);
					}
				}
				
			}
			
			Map<String, ColumnDescriptor> mapsDst = new HashMap<String, ColumnDescriptor>(dstCols.size(), 1f);
			for (ColumnDescriptor dst : dstCols) {
				mapsDst.put(dst.getName(), dst);
			}
		
			for (Iterator<ColumnDescriptor> iterSrcCol = srcCols.iterator(); iterSrcCol.hasNext();) {
				ColumnDescriptor src = iterSrcCol.next();
				ColumnDescriptor dst = mapsDst.get(src.getName());
				if (	
						dst!=null && (
						!src.getClassName().equals(dst.getClassName()) ||
						!src.getColumnTypeName().equals(dst.getColumnTypeName()) ||
						src.getNumber() != dst.getNumber() 
					)) {
					LOGGER.error("COLUMN {} OF TABLE {} ARE DIFFERENT: src:{},{} ; dst:{},{}", src.getName(), table
							, src.getClassName(), src.getColumnTypeName(), dst.getClassName(), dst.getColumnTypeName() );
					csv.print("COLUMNS DIFFERENT", "WARN", table, src, dst);
				}
			}
			
		}
		
		csv.close();
	}

	/*
	int pos = Collections.binarySearch(regroupement, rgmnt, PmRegroupementComparator.INSTANCE);
    //l'article est encore dans un regroupement travaill�
    if (pos>=0) {
      item.setPrixMagasin(bomPrixMag.getPrixArticle());
    }
    */


    /**
     * innert class pour tri des liste de desc de colonnes par ordre et colonne JDBC.
     * @author cunvoas
     */
    static class ColumnDescriptorComparator implements Comparator<ColumnDescriptor> {
      static final ColumnDescriptorComparator  INSTANCE = new ColumnDescriptorComparator();
      
      /**
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(ColumnDescriptor o1, ColumnDescriptor o2) {
    	  CompareToBuilder ctp = new CompareToBuilder();
    	  ctp.append(o1.getName(), o2.getName());
        return  ctp.toComparison();
      }
    }
	
    
    
	
}
