package com.demo.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.util.PhoenixRuntime;

/******************************************
Created by: Borong Zhou
Created at: Sep 10, 2013:9:43:04 AM 
File: MyInsertion.java
Comments:

 ******************************************/
public class MyInsertion {

	private static SimpleDateFormat dashFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS"); // host analytics
	private static SimpleDateFormat slashFormat = new SimpleDateFormat("yyyyMM"); // hh:mm:ss.SSS"); // replicon
	// 2012/01/11 00:00:00.000
	private static String BNA_USAGE = "BNA_USAGE";
	private static final int BATCH_SIZE = 3000;
	
	public static void main(String[] args) {
//		String filepath = "/Users/borongzhou/test/hostAnalysis/import";
//		String logfile = "/Users/borongzhou/test/hostAnalysis/output.log";
//		int custType = CUST_NAME.HOST_ANALYTICS.ordinal();
		
		new MyInsertion().retrieval();
		
//		try {
//			new MyInsertion().load(args);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws InterruptedException 
	 */
	public void load(String[] args) throws IOException, ParseException, InterruptedException {
		String filepath = args[0];
		String logfile = args[1];
		int custType = Integer.parseInt(args[2]);
		File folder = new File(filepath);
		
		String[] files = folder.list();
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(logfile));
			for (String filename : files) {
				System.out.printf("%s is processing!\n", filename);
				bulkImport(filepath + File.separator + filename, custType);
				
				bw.write(filename + " has been processed!\n");
				System.out.printf("%s has been processed!\n", filename);
				Thread.currentThread().sleep(100);
			}
			
			bw.close();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}
	
	public void retrieval() {
		
		try {
			Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
			String query = "select event, count(1) as cnt from BNA_USAGE where aid='5238035df7865e4b3319259d' group by event order by cnt;";
			query = "select avg(cnt) as cnt from (select TO_CHAR(CREATED, 'yyyyMM') as datekey, count(EVENT) as cnt from BNA_USAGE where aid='103209' group by TO_CHAR(CREATED, 'yyyy/MM/dd') order by cnt);";
			query = "select count(1) from BNA_USAGE;";
			PhoenixConnection conn = DriverManager.getConnection("jdbc:phoenix:10.0.3.185").unwrap(PhoenixConnection.class);
	        PhoenixRuntime.executeStatements(conn, new StringReader(query), null);
	        conn.close();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	void bulkImport(String filename, int custType) throws ClassNotFoundException, SQLException, IOException, ParseException {
		
		Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
        StringBuilder buf = new StringBuilder();

        buf.append("CREATE TABLE IF NOT EXISTS ")
        .append(BNA_USAGE).append("(")
        .append("CID VARCHAR NOT NULL,")
        .append("AID VARCHAR NOT NULL,")
        .append("ANAME VARCHAR,")
        .append("UNAME VARCHAR NOT NULL,")
        .append("PROD VARCHAR NOT NULL,")
        .append("COMP VARCHAR NOT NULL,")
        .append("EVENT VARCHAR NOT NULL,")
        .append("CREATED DATE NOT  NULL,")
        .append("DESCRIPTION VARCHAR ")
        .append("CONSTRAINT USAGE_PK PRIMARY KEY (CID,AID,UNAME,PROD,COMP,EVENT,CREATED)")
        .append(");")
        ;
        // ec2-54-218-194-228.us-west-2.compute.amazonaws.com 10.0.9.7
        PhoenixConnection conn = DriverManager.getConnection("jdbc:phoenix:10.0.3.185").unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(buf.toString()), null);
        conn.setAutoCommit(false);
        
        int count = BATCH_SIZE;
        
		File sFile = new File(filename);
		BufferedReader br = new BufferedReader(new FileReader(sFile));

		String line = null;
		String[] splits = null;
		PreparedStatement upsertStmt = null;
		StringBuilder sql = new StringBuilder(); 
		while ((line = br.readLine()) != null) {
			splits = line.split("\t");
			if (splits == null || splits.length != HOST_RAW_USAGE.values().length) {
				System.out.printf("Invalid line %s\n", line);
				continue;
			}

	        String upsert = "UPSERT INTO " + BNA_USAGE + "(cid,aid,aname,uname,prod,comp,event,created,description) " +
	            " VALUES(?,?,?,?,?,?,?,?,?)";
	        
			upsertStmt = conn.prepareStatement(upsert);
			if (custType == CUST_NAME.HOST_ANALYTICS.ordinal())
				upsertStmt.setString(1, "524c9ffbf7864895bdd8ee6c");
			else if (custType == CUST_NAME.REPLICON.ordinal())
				upsertStmt.setString(1, "524c9ffbf7864895bdd8ee6d"); 
			// host:  5229f0663004e751ecdf841f; replicon: 5229f0663004e751ecdf8420
	        upsertStmt.setString(2, splits[HOST_RAW_USAGE.acctid.ordinal()]);
	        upsertStmt.setString(3, splits[HOST_RAW_USAGE.acctname.ordinal()]);
	        upsertStmt.setString(4, splits[HOST_RAW_USAGE.username.ordinal()]);
	        upsertStmt.setString(5, splits[HOST_RAW_USAGE.prodname.ordinal()]);
	        upsertStmt.setString(6, splits[HOST_RAW_USAGE.comp.ordinal()]);
	        upsertStmt.setString(7, splits[HOST_RAW_USAGE.eventname.ordinal()]);

			if (custType == CUST_NAME.HOST_ANALYTICS.ordinal())
				upsertStmt.setDate(8, new Date(dashFormat.parse(splits[HOST_RAW_USAGE.createdt.ordinal()]).getTime()));
			else if (custType == CUST_NAME.REPLICON.ordinal())
				upsertStmt.setDate(8, new Date(slashFormat.parse(splits[HOST_RAW_USAGE.createdt.ordinal()]).getTime()));
	        upsertStmt.setString(9, splits[HOST_RAW_USAGE.description.ordinal()]);
	        
	        upsertStmt.executeUpdate();
	        
	        if (count-- == 0) {
	            conn.commit();
	            count = BATCH_SIZE; 
	            System.out.printf("%d records loaded from file %s\n", BATCH_SIZE, filename);
	        }
		}
        
		if (count < BATCH_SIZE) {
			conn.commit(); 
			System.out.printf("last %d records loaded from file %s\n", BATCH_SIZE - count + 1, filename);
		}
		
        conn.close();
        
        br.close();
        
        return;
	}

	
	public static enum CUST_NAME {
		HOST_ANALYTICS,
		REPLICON,
		BRIGHTIDEA
	}
	
	
	public static enum HOST_RAW_USAGE {
		custid,
		acctid,
		acctname,
		username,
		prodname,
		comp,
		eventname,
		createdt,
		description
	}
	
}
