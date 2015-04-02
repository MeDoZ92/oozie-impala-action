package com.cloudera.fce;

import java.io.BufferedReader;
import java.io.FileReader;
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

	private static final String SQL_STATEMENT = "create table diediedie (x int)";
	private static final String CONNECTION_URL = "jdbc:hive2://172.16.58.203:21050/default;auth=noSasl";
	// private static final String CONNECTION_URL = "jdbc:impala://172.16.58.203:21050/default;auth=noSasl";
	private static final String CONNECTION_USER = ".";
	private static final String CONNECTION_PASS = ".";
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	// private static final String JDBC_DRIVER_NAME = "com.cloudera.impala.jdbc4.Driver";

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

	public static void main(String[] args) throws IllegalArgumentException, IOException, SQLException {

    if( args.length != 2 ){
      String usage = "Usage: <query-file> <connection-url>";
      System.out.println(usage);
      throw new IllegalArgumentException(usage);
    }

    String queryFile     = args[0];
    String connectionUrl = args[1];
		Connection con       = null;

		try {
      String sqlStatement  = retrieveFileContents( queryFile );
      System.out.println("\n=============================================");
      System.out.println("Cloudera Impala JDBC Example");
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
