package com.cloudera.fce;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

// Inputs
//  - ConnectionURL
//  - QueryFile
//  - Keytab & Service Credential to Use
//  - Driver [optional]

public class ClouderaImpalaJdbcExample {

	private static final String CONNECTION_USER = ".";
	private static final String CONNECTION_PASS = ".";
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	private static final String JAAS_FILE = "impala-action-jaas.conf";

  private static String retrieveFileContents(String filename) throws IOException {
    String contents = null;
    BufferedReader br = new BufferedReader(new FileReader(filename));
    try {
        StringBuilder sb = new StringBuilder();
        String line = br.readLine();
        while (line != null) {
            sb.append(line);
            sb.append(System.lineSeparator());
            line = br.readLine();
        }
        contents = sb.toString();
    } finally {
        br.close();
    }
    return contents;
  }

  private static boolean fileExists(String filename) {
    File f = new File(filename);
    return (f.exists() && !f.isDirectory());
  }

private static String createJaasFileConf(String keytabFile, String kerberosPrinc) throws IOException {
    PrintWriter w = new PrintWriter(JAAS_FILE, "UTF-8");
    w.println("com.sun.security.jgss.initiate {");
    w.println("  com.sun.security.auth.module.Krb5LoginModule required");
    w.println("  useKeyTab=true");
    w.println("  keyTab=\"" + keytabFile + "\"");
    w.println("  storeKey=true");
    w.println("  useTicketCache=false");
    w.println("  principal=\"" + kerberosPrinc + "\"");
    w.println("  doNotPrompt=true");
    w.println("  debug=true;");
    w.println("};");
    w.close();
    return JAAS_FILE;
  }

	public static void main(String[] args) throws IllegalArgumentException, IOException, SQLException, ClassNotFoundException {

    if( args.length != 2 && args.length != 4 ){
      String usage = "Usage: <query-file> <connection-url> [keytab-file] [principal]";
      System.out.println(usage);
      throw new IllegalArgumentException(usage);
    }

    String queryFile     = args[0];
    String connectionUrl = args[1];
    String keytabFile    = ( args.length == 4 ) ? args[2] : null;
    String kerberosPrinc = ( args.length == 4 ) ? args[3] : null;
		Connection con       = null;

    if( !fileExists(args[0]) ){
      throw new IllegalArgumentException("Specified [query-file] does not exist");
    }
    if ( keytabFile != null && !fileExists(keytabFile) ){
      throw new IllegalArgumentException("Specified [keytab-file] does not exist");
    }
    if ( keytabFile != null ) {
      String jaas_file = createJaasFileConf(keytabFile, kerberosPrinc);
      System.setProperty("java.security.auth.login.config", jaas_file);
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      if( System.getProperty("java.security.krb5.conf") == null ){
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
      }
    }

		try {
      String sqlStatement  = retrieveFileContents( queryFile );
      System.out.println("\n=============================================");
      System.out.println("Impala JDBC Oozie Action");
      System.out.println("Using Connection URL: " + connectionUrl);
      System.out.println("Running Query: " + sqlStatement);
      Class.forName(JDBC_DRIVER_NAME);

      con                    = DriverManager.getConnection(connectionUrl, CONNECTION_USER, CONNECTION_PASS);
      PreparedStatement stmt = con.prepareStatement(sqlStatement);
      boolean success        = stmt.execute();
      System.out.println("Query Finished: " + success);
		} finally {
				con.close();
		}
	}
}
