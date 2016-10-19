package com.github.cunvoas.dbmigration;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.cunvoas.dbmigration.config.ExclusionConfig;
import com.github.cunvoas.dbmigration.config.MappingExclusion;
import com.github.cunvoas.dbmigration.config.MappingExclusionReader;
import com.github.cunvoas.dbmigration.db.AuditBase;
import com.github.cunvoas.dbmigration.db.MigrateData;
import com.github.cunvoas.dbmigration.util.ConnectionConfig;
import com.github.cunvoas.dbmigration.util.ConnectionProvider;
import com.github.cunvoas.dbmigration.util.MigrationException;

/**
 * @author cunvoas
 */
public class ApplicationMigration {
	private static final Logger LOGGER = LoggerFactory .getLogger(ApplicationMigration.class);
	
	public static final int CODE_RETOUR_OK=0;
	public static final int CODE_RETOUR_WARN=1;
	public static final int CODE_RETOUR_ERROR=2;
	
	private static final String EMPTY_STRING="NULL";
	private static final String AD_STRING="AD";
	
	
	private static final String SRC_DB = "src_base";
	private static final String SRC_DC = "src_class";
	private static final String SRC_DS = "src_datasource";
	private static final String SRC_USR = "src_user";
	private static final String SRC_PWD = "src_pass";
	
	private static final String DST_DB = "dst_base";
	private static final String DST_DC = "dst_class";
	private static final String DST_DS = "dst_datasource";
	private static final String DST_USR = "dst_user";
	private static final String DST_PWD = "dst_pass";
	
	private static final String CFG_FILE = "exclusions";
	
	
	public static final String MODE_AUDIT="audit";
	public static final String MODE_RECOPIE="recopie";
	
	private static final String MODE = "mode";	// audit | recopie

	private static Options options = null; // Command line options
	private static CommandLine cmd = null; // Command Line arguments
	

	static {
		options = new Options();
		options.addOption(SRC_DB, true, "Nom de la base source");
		options.addOption(SRC_DC, true, "Classe du driver JDBC de la base Source");
		options.addOption(SRC_DS, true, "URL JDBC de la datasource de la Source");
		options.addOption(SRC_USR, true, "Utilisateur de la base Source");
		options.addOption(SRC_PWD, true, "Mot de passe de la base Source");
		
		options.addOption(DST_DB, true, "Nom de la base source");
		options.addOption(DST_DC, true, "Classe du driver JDBC de la base Source");
		options.addOption(DST_DS, true, "URL JDBC de la datasource de la Source");
		options.addOption(DST_USR, true, "Utilisateur de la base Source");
		options.addOption(DST_PWD, true, "Mot de passe de la base Source");
		
		options.addOption(MODE, true, "Mode d'utilisation de l'outil de migration : audit | recopie");
		
		options.addOption(CFG_FILE, true, "Fichier d'exclusion des tables et colonnes sources.");
	}
	

	private static ConnectionConfig source = null;
	private static ConnectionConfig destination = null;
	private static String mode = null;
	
	
	/**
	 * Chargement et controle des arguments.
	 * 
	 * @param args
	 */
	private static void loadArgs(String[] args) throws MigrationException {
		LOGGER.info("parse arguments");
		CommandLineParser parser = new PosixParser();

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("Error parsing arguments");
			e.printStackTrace();
			System.exit(CODE_RETOUR_ERROR);
		}

		// Check for mandatory args

		if (       !cmd.hasOption(SRC_DB)
				|| !cmd.hasOption(SRC_DC)
				|| !cmd.hasOption(SRC_DS)
				|| !cmd.hasOption(SRC_USR)
				|| !cmd.hasOption(SRC_PWD)
				
				|| !cmd.hasOption(DST_DB)
				|| !cmd.hasOption(DST_DC)
				|| !cmd.hasOption(DST_DS)
				|| !cmd.hasOption(DST_USR)
				|| !cmd.hasOption(DST_PWD)
				
				|| !cmd.hasOption(MODE)
				
				) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar dbMigration", options);
			System.exit(CODE_RETOUR_ERROR);
		}

		mode = cmd.getOptionValue(MODE).toLowerCase();
		if (!MODE_AUDIT.equals(mode) && !MODE_RECOPIE.equals(mode)) {
			System.out.println(String.format("le mode est '%s' OU '%s'", MODE_AUDIT, MODE_RECOPIE));
			System.exit(CODE_RETOUR_ERROR);
		}
		String user = null;
		String pass = null;
		
		source = new ConnectionConfig();
		source.setDatabase(cmd.getOptionValue(SRC_DB));
		source.setDriverName(cmd.getOptionValue(SRC_DC));
		source.setDatasource(cmd.getOptionValue(SRC_DS));
		user = cmd.getOptionValue(SRC_USR);
		if (!AD_STRING.equals(user)) {
			pass = cmd.getOptionValue(SRC_PWD);
			
			if (pass==null || EMPTY_STRING.equals(pass)) {
				pass= StringUtils.EMPTY;
			}
			source.setPass(pass);
		}		
		
		destination = new ConnectionConfig();
		destination.setDatabase(cmd.getOptionValue(DST_DB));
		destination.setDriverName(cmd.getOptionValue(DST_DC));
		destination.setDatasource(cmd.getOptionValue(DST_DS));
		destination.setUser(cmd.getOptionValue(DST_USR));
		pass = cmd.getOptionValue(DST_PWD);
		if ("NULL".equals(pass)) {
			pass= StringUtils.EMPTY;
		}
		destination.setPass(pass);
		
		String config = cmd.getOptionValue(CFG_FILE);
		if (config!=null && (new File(config)).isFile()) {
			MappingExclusionReader reader = new MappingExclusionReader();
			List<MappingExclusion> exclusions = reader.read(new File(config));
			ExclusionConfig.getINSTANCE().setExclusion(exclusions);
		}
		
		LOGGER.info("parse arguments done; mode={}", mode);
	}

	public static void main(String[] args) {
		int codeRetour = CODE_RETOUR_ERROR;
		
		try {
			loadArgs(args);
			
			if (MODE_AUDIT.equals(mode)) {
				AuditBase auditBase = new AuditBase();
				auditBase.audit(
						ConnectionProvider.getConnection(source), 
						ConnectionProvider.getConnection(destination),
						destination.getDatabase());
				
			} else if (MODE_RECOPIE.equals(mode)) {
								
				MigrateData migrateBaseData = new MigrateData();
				migrateBaseData.migrate(source, destination);
				
			}
			
			codeRetour = CODE_RETOUR_OK;
		} catch (Exception e) {
			codeRetour = CODE_RETOUR_ERROR;
			LOGGER.error("imprevu", e);
		} finally {
			System.exit(codeRetour);
		}
	}
}
