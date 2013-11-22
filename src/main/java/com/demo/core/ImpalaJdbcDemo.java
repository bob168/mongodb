package com.demo.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/******************************************
Created by: Borong Zhou
Created at: Nov 11, 2013:10:07:46 PM 
File: ImpalaJdbcDemo.java
Comments:
 ******************************************/

public class ImpalaJdbcDemo {
    // SAAS Data Services
    private static final String SQL_STATEMENT = "SELECT count(1) as cnt,eventName from default.fakerawdata where compName='analyze' group by eventName order by cnt desc, eventName limit 99999999";
    // "SELECT count(1) as cnt,acctName from default.fakerawdata group by acctName order by cnt desc, acctName limit 99999999";
    // "SELECT count(distinct niceid) as cnt, acctname from brightticket a, brightnmacct b where a.orgid=b.acctId group by acctname order by cnt desc limit 9999999999"; 
    // "SELECT count(1) as cnt,yweek,ylweek,eventName from default.fakerawdata where compName='analyze' group by ylweek,yweek,eventName order by cnt desc,yweek limit 9999999999"; 
    // "SELECT count(1) as cnt,yweek,ylweek,eventName from default.fakerawdata where prodName='SAAS Data Services' group by ylweek,yweek,eventName order by yweek,cnt desc limit 99999999"; 
    // "SELECT count(1) as cnt,yweek,ylweek,eventName from default.fakerawdata where userName='Catherine Hilyard' group by ylweek,yweek,eventName order by yweek,cnt desc limit 99999999"; 
    // "SELECT count(1) as cnt,yweek,ylweek,eventName from default.fakerawdata where acctId='878' group by ylweek,yweek,eventName order by yweek,cnt desc limit 99999999"; // "SELECT * FROM bnademo.tab1 limit 10";
    
    private static final String IMPALAD_HOST = "10.0.3.186";
    private static final String IMPALAD_JDBC_PORT = "21050";
    private static final String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";
//    		"jdbc:hive2://10.0.3.186:21050/;auth=noSasl";
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) {

		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example");
		System.out.println("Using Connection URL: " + CONNECTION_URL);
		System.out.println("Running Query: " + SQL_STATEMENT);

		Connection con = null;

		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(CONNECTION_URL);
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(SQL_STATEMENT);
			long startDT = System.currentTimeMillis();
			System.out.println("\n== Begin Query Results ============\n");

			while (rs.next()) {
				System.out.printf("%d\t%s\n", rs.getInt(1), rs.getString(2));
//				System.out.printf("%d\t%d\t%d\t%s\n", rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getString(4));
			}
			System.out.printf("== Query Time in sec: %dms\n\n", (int)(System.currentTimeMillis() - startDT));

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
		}
	}
}
