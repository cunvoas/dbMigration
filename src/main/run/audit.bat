
java -jar dbMigration.jar -src_base Drupal_ACS -src_class com.microsoft.sqlserver.jdbc.SQLServerDriver -src_datasource jdbc:sqlserver://st017547:1433;databaseName=Drupal_ACS -src_user usertest -src_pass mdp -dst_base autonomie -dst_class org.mariadb.jdbc.Driver -dst_datasource jdbc:mariadb://localhost:3306/autonomie -dst_user root -dst_pass  -mode audit
