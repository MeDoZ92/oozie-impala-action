package com.cloudera.fce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

// Inputs
//  - ConnectionURL
//  - QueryFile
//  - Keytab & Service Credential to Use
//  - Driver [optional]

public class ClouderaImpalaJdbcExample {

	private static final String SQL_STATEMENT = "show tables";
	private static final String CONNECTION_URL = "jdbc:hive2://172.16.58.203:21050/default;auth=noSasl";
	// private static final String CONNECTION_URL = "jdbc:impala://172.16.58.203:21050/default;auth=noSasl";
	private static final String CONNECTION_USER = ".";
	private static final String CONNECTION_PASS = ".";
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	// private static final String JDBC_DRIVER_NAME = "com.cloudera.impala.jdbc4.Driver";

	public static void main(String[] args) {

		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example");
		System.out.println("Using Connection URL: " + CONNECTION_URL);
		System.out.println("Running Query: " + SQL_STATEMENT);
		Connection con = null;

		try {

			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(CONNECTION_URL, CONNECTION_USER, CONNECTION_PASS);
			PreparedStatement stmt = con.prepareStatement(SQL_STATEMENT);
			ResultSet rs = stmt.executeQuery();

			System.out.println("\n== Begin Query Results ======================");
			while (rs.next()) {
				System.out.println(rs.getString(1));
				System.out.println();
			}
			System.out.println("== End Query Results =======================\n\n");

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
        e.printStackTrace();
			}
		}
	}
}
