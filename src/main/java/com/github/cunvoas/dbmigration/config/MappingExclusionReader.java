/**
 * 
 */
package com.github.cunvoas.dbmigration.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.cunvoas.dbmigration.util.MigrationException;

/**
 * @author UNVOAS
 */
public class MappingExclusionReader {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MappingExclusionReader.class);
	
	/**
	 * Read and parse config file.
	 * @param configFile
	 * @return
	 * @throws MigrationException
	 */
	public List<MappingExclusion> read(File configFile) throws MigrationException{
		List<MappingExclusion> ret = new ArrayList<MappingExclusion>();
		
		try {
			List<String> LineSeparatorConverter = FileUtils.readLines(configFile);
			for (String string : LineSeparatorConverter) {
				String[] tc = string.split("\\.");
				MappingExclusion me = new MappingExclusion();
				me.setTable(tc[0].toLowerCase());
				me.setColumn(tc[1].toLowerCase());
				ret.add(me);
			}
			
		} catch (IOException e) {
			try {
				LOGGER.error("file not found {}", configFile.getCanonicalPath());
			} catch (IOException e1) {
				LOGGER.error("ioe err", e);
			}
			throw new MigrationException("ile not found", e);
		}
		return ret;
	}
}
