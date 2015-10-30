package com.mapr.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveTableCreateDataLoadAndFetch {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		Connection con = null;
		try {
			// Register driver and create driver instance
			Class.forName(driverName);
			con = DriverManager.getConnection(
					"jdbc:hive://localhost:10000/thanooj", "", "");
			Statement stmt = con.createStatement();

			boolean execute = stmt.execute("DROP TABLE IF EXISTS thanooj.employee;");
			if(execute){
				System.out.println("thanooj.employee, is already exist, table has been dropped.)");
			}
			System.out.println("Table employee creation started.");
			
			boolean execute2 = stmt.execute("CREATE TABLE IF NOT EXISTS "
					+ " thanooj.employee ( eid int, name String, "
					+ " gender String, doj DATE, "
					+ " salary double, destignation String, deptid int)"
					+ " COMMENT 'Employee details'" + " ROW FORMAT DELIMITED"
					+ " FIELDS TERMINATED BY ','" + " LINES TERMINATED BY '\n'"
					+ " STORED AS TEXTFILE;");
			if(execute2){
			System.out.println("Table employee created successful.");
			}
			
			System.out.println("Load Data into employee stated");
			
			boolean execute3 = stmt.execute("LOAD DATA LOCAL INPATH '/home/ubuntu/input/employee.txt'"
					+ "OVERWRITE INTO TABLE employee;");
			if(execute3){
			System.out.println("Load Data into employee successful");
			}

			ResultSet res = stmt
					.executeQuery("SELECT * FROM thanooj.employee ORDER BY deptid;");
			
			System.out.println("HiveTableCreateDataLoadAndFetch.main() - select query");
			while (res.next()) {
				System.out.println(res.getInt(1) + " " + res.getString(2) + " "
						+ res.getString(3) + " " + res.getDate(4) + " "
						+ res.getDouble(5) + " " + res.getString(6) + " "
						+ res.getInt(7));
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			con.close();
		}

	}
}
