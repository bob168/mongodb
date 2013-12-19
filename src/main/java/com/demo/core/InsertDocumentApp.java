package com.demo.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bluenose.powder.util.CommonTokens;
import com.bluenose.powder.util.StringUtil;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
 
/**
 * Java MongoDB : Insert a Document into collection
 * 
 */
public class InsertDocumentApp {
	
	private static final ObjectId _idGenerator = new ObjectId();
	private static Map<String, String> AcctMappping = null;
	private static Map<String, Double> AcctScoring = null;
	private static Map<String, FirstLastEvent> AcctDates = null;
	private static Map<String, FirstLastEvent> UserDates = null;
	private static Map<String, AcctDates> ChurnRenewalDates = null;

	private static final String ipAddr = "10.0.9.21"; // Haley-21, Curtls-23, Sameer-24 Dem-13 miles-26 todd-15 "ec2-54-214-129-200.us-west-2.compute.amazonaws.com"; //"10.0.9.2"; //"ec2-23-22-156-75.compute-1.amazonaws.com";
	private static final int port = 27017; //37017; // 27017; //  
	//TODO:: fa8e12345678900000000006 for Todd's cloudPassage; fa8e12345678900000000007 for Todd's new BrightIdea
	private static final String dbName = "bow-5229f0663004e751ecdf841c"; //demo "bow-5229f0663004e751ecdf841c"; //brightidea "bow-5229f0663004e751ecdf8425"; // "bow-replicon"; //"bow-brightidea"; // "bow-fa8e12345678900000000001"; //"bow-fa8e12345678900000000001"; //"bow-replicon"; //"bow-bna"; // "bow-fa8e12345678900000000000"; // "bow-openvpn"; //
	private static final String username = "bnaadmin";
	private static final String password = "bluenose!";
	

	private Map<String, LinkedHashSet<Survey>> acctSurveys = null;		
	private Map<String, LinkedHashSet<Campaign>> acctCampaigns = null;		
	private Map<String, LinkedHashSet<Note>> acctNotes = null;		
	private Map<String, LinkedHashSet<Interaction>> acctInteractions = null;		
	private Map<String, LinkedHashSet<FeatureReq>> acctFeatureReqs = null;

	private Map<String, LinkedHashSet<Health>> acctsHScores = null;
	private Map<String, LinkedHashSet<Opportunity>> acctOppties = null;
	private Map<String, LinkedHashSet<Ticket>> acctTickets = null;
	private Map<String, LinkedHashSet<Finance>> acctFinances = null;
	private Map<String, LinkedHashSet<Health>> usersHScores = null;
	
	private Map<String, LinkedHashSet<Entitlement>> acctEntitlements = null;
	private Map<String, LinkedHashSet<Invoice>> acctInvoices = null;
	private Map<String, LinkedHashSet<Subscription>> acctSubscriptions = null;
	
	private static SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMdd");
	private static DecimalFormat dFormat = new DecimalFormat("################.##");
	private static SimpleDateFormat usageDateFormat = new SimpleDateFormat("yyyyMMdd"); //"yyyy-MM-dd hh:mm:ss.SSS");
	private static SimpleDateFormat usageDashDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat usageMonthFormat = new SimpleDateFormat("yyyyMM"); //"yyyy-MM-dd hh:mm:ss.SSS");
	private static SimpleDateFormat usageFullDateFormat = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
	private static SimpleDateFormat usageHourFormat = new SimpleDateFormat("yyyyMMdd hh");

	private static SimpleDateFormat ticketDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	private static final Gson GSON = new Gson();

	public static void main(String[] args) throws IOException  {
		
//		File sFile = new File("/Users/borongzhou/test/replicon/product/endUserFromUsage.tsv");
//		BufferedReader br = new BufferedReader(new FileReader(sFile));
//		
//		String line = null;
//		
//		while((line = br.readLine()) != null) {
//			String[] splits = line.split("\t");
//			
//			if ("521093".equals(splits[0])) {
//				
//				
//				System.out.printf("there is the data %s for 521093 with fist=%s\t2nd=%s\n", line,splits[0],splits[1]);
//			}
//		}
//		
//		br.close();

//		System.out.println(_idGenerator);
		InsertDocumentApp app = new InsertDocumentApp();
		app.setup();
		app.insertBNAdemo();
//		app.insertBrightidea();
//		app.insertCloudPassage();
//		app.insertReplicon();
		app.tearDown();
		
//		try {
//			// mongodb://admin:password@localhost:27017/db
////			MongoClient.connect(addr);
//			
//			MongoClient mongoClient = new MongoClient(ipAddr, port);
//			DB db = mongoClient.getDB("bow-openvpn");
//			boolean auth = db.authenticate(username, password.toCharArray());
//			if (!auth) {
//				System.out.println("authentication error!");
//				System.exit(1);
//			}
//			
//			DBCollection collection = db.getCollection("account");
//			collection.drop();
//			collection = db.getCollection("account");
//
//			InsertDocumentApp app = new InsertDocumentApp();
//			app.setup();
//			app.insertOpenVPN();
//			app.tearDown();
//			
////			// 2. BasicDBObjectBuilder example
////			System.out.println("BasicDBObjectBuilder example...");
////			BasicDBObjectBuilder documentBuilder = BasicDBObjectBuilder.start()
////					.add("database", "bow").add("table", "account");
////
////			BasicDBObjectBuilder documentBuilderDetail = BasicDBObjectBuilder
////					.start().add("_id", new ObjectId("5229f0663004e751ecdf841c")).add("records", "99")
////					.add("index", "vps_index1").add("active", "true");
////
////			documentBuilder.add("detail", documentBuilderDetail.get());
////
////			collection.insert(documentBuilder.get());
////
////			DBCursor cursorDocBuilder = collection.find();
////			while (cursorDocBuilder.hasNext()) {
////				System.out.println(cursorDocBuilder.next());
////			}
////
////			collection.remove(new BasicDBObject());
//			
////			
////			// 3. Map example
////			System.out.println("Map example...");
////			Map<String, Object> documentMap = new HashMap<String, Object>();
////			documentMap.put("database", "bow");
////			documentMap.put("table", "hosting");
////
////			Map<String, Object> documentMapDetail = new HashMap<String, Object>();
////			documentMapDetail.put("records", "99");
////			documentMapDetail.put("index", "vps_index1");
////			documentMapDetail.put("active", "true");
////
////			documentMap.put("detail", documentMapDetail);
////
////			collection.insert(new BasicDBObject(documentMap));
////
////			DBCursor cursorDocMap = collection.find();
////			while (cursorDocMap.hasNext()) {
////				System.out.println(cursorDocMap.next());
////			}
////
////			collection.remove(new BasicDBObject());
//
//		} catch(Throwable ex) {
//			ex.printStackTrace(System.out);
//		}
	}

	@Test
	public void retrieve() {

		try {
			Mongo mongo = new Mongo(ipAddr, port);
			DB db = mongo.getDB(dbName);
			
//			MongoClient mongoClient = new MongoClient(ipAddr, port);
//			DB db = mongoClient.getDB(dbName);
//			boolean auth = db.authenticate(username, password.toCharArray());
//			if (!auth) {
//				System.out.println("authentication error!");
//				System.exit(1);	
//			}
			
			DBCollection account = db.getCollection("account");
			
			BasicDBObject whereQuery = new BasicDBObject();
//			whereQuery.put("name", "vamsi krishna"); 
//			whereQuery.put("accountId", new ObjectId("52965ceaf7864ff1919df558")); 
			whereQuery.put("name", "A. Schulman");
			DBCursor cursor = account.find(whereQuery);

			System.out.println(account.count());
			
			int count = 10;
			while (count-- > 0 && cursor.hasNext()) {
				System.out.println(cursor.next());
			}
			cursor.close();
			
			DBCollection user = db.getCollection("endUser");
			whereQuery = new BasicDBObject();
//			whereQuery.put("name", "mirion technologies inc");
//			whereQuery.put("_id", new ObjectId("5255f58af786e6b83896681a"));
			whereQuery.put("accountId", new ObjectId("52b33bc5300422091bce28d5"));
			DBCursor cursorDoc = user.find(whereQuery);
			count = 10;
			while (count--> 0 && cursorDoc.hasNext()) {
				System.out.println(cursorDoc.next());
			}

			System.out.println(user.count());
			cursorDoc.close();
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	
//	@Test
	public void insertOpenVPN()  {
		
		try {
			insertOpenAcctHealthScore("/Users/borongzhou/test/openVPN/acctHealthScores.tsv");
			insertOpenAccountObject("/Users/borongzhou/test/openVPN/acctsFromAcct.tsv");
			
		} catch(Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public void insertBNAdemo()  {
		
		try {
			loadEvents(CUSTOMER_TYPE.BNA_DEMO.ordinal());
			// TODO:: repScoreExt_v1.tsv
			insertGeneralAcctHealthScore("/Users/borongzhou/test/fake/product/acctHealthScoreExt.tsv", CUSTOMER_TYPE.BNA_DEMO.ordinal());
//			insertAcctHealthScore("/Users/borongzhou/test/fake/product/acctHealthScoreExt.tsv"); //TODO:: This is acceptable one repScoreExt_v1.tsv"); // acctHealthScoreExt.tsv");
			insertGeneralAccountObject("/Users/borongzhou/test/fake/product/acctsFromAcct.tsv", CUSTOMER_TYPE.BNA_DEMO.ordinal());
			acctsHScores.clear();
			acctsHScores = null;
		
//			insertUserHealthScore("/Users/borongzhou/test/fake/product/endUserHealthScoreExt.tsv");
			insertBNAenduserObject("/Users/borongzhou/test/fake/product/endUserFromUsage.tsv", CUSTOMER_TYPE.BNA_DEMO.ordinal());
			
			usersHScores.clear();
			usersHScores = null;
			
		} catch(Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public void insertBrightidea()  {
		
		try {
			loadAcctEvents("/Users/borongzhou/test/brightidea/product/acctFirstLastDate.tsv");
			insertGeneralAcctHealthScore("/Users/borongzhou/test/brightidea/product/brightScoreExt.tsv", CUSTOMER_TYPE.BRIGHTIDEA.ordinal());
			insertTicketObject("/Users/borongzhou/test/brightidea/product/ticketObj.tsv");
			insertGeneralAccountObject("/Users/borongzhou/test/brightidea/product/acctsFromAcct.tsv", CUSTOMER_TYPE.BRIGHTIDEA.ordinal());
			
		} catch(Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public void insertCloudPassage()  {
		
		try {
			loadAcctEvents("/Users/borongzhou/test/cloudPassage/product/acctFirstLastDate.tsv");
			insertGeneralAcctHealthScore("/Users/borongzhou/test/cloudPassage/product/cloudScoreExt.tsv", CUSTOMER_TYPE.CLOUDPASSAGE.ordinal());
			insertGeneralAccountObject("/Users/borongzhou/test/cloudPassage/product/acctsFromAcct.tsv", CUSTOMER_TYPE.CLOUDPASSAGE.ordinal());
			
		} catch(Exception ex) {
			ex.printStackTrace(System.out);
		}
	}
	
//	@Test
	public void insertReplicon()  {
		
		try {
			loadEvents(CUSTOMER_TYPE.REPLICON.ordinal());
			// TODO:: repScoreExt_v1.tsv
			insertAcctHealthScore("/Users/borongzhou/test/replicon/product/repScoreExt_v1.tsv"); //TODO:: This is acceptable one repScoreExt_v1.tsv"); // acctHealthScoreExt.tsv");
//			insertOpportunityObject("/Users/borongzhou/test/bnaAnalytics/product2/hostOppty.tsv");
			insertBNAaccountObject("/Users/borongzhou/test/replicon/product/acctsFromAcct.tsv", "524c9ffbf7864895bdd8ee6d"); //"524c9ffbf7864895bdd8ee69");
			acctsHScores.clear();
			acctsHScores = null;
//			acctOppties.clear();
//			acctOppties = null;
		
			insertUserHealthScore("/Users/borongzhou/test/replicon/product/endUserHealthScoreExt.tsv");
			insertBNAenduserObject("/Users/borongzhou/test/replicon/product/endUserFromUsage.tsv", 0);
			
			usersHScores.clear();
			usersHScores = null;
		} catch(Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	@Before
	public void setup() {

		try {
			Mongo mongo = new Mongo(ipAddr, port);
			DB db = mongo.getDB(dbName);
			
			// dbName
//			db.dropDatabase();
//			mongo.getDB(dbName);
			// set customer object
			DBCollection table = db.getCollection("customer");
			table.drop();
			table = db.getCollection("customer");
			
			BasicDBObject mydbObject = new BasicDBObject();
			mydbObject.put("_id", dbName.replaceFirst("bow-", ""));
			
			table.insert(mydbObject);
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
		
		AcctDates = new HashMap<String, FirstLastEvent>();
		UserDates = new HashMap<String, FirstLastEvent>();
		AcctMappping = new HashMap<String, String>();
		AcctScoring = new HashMap<String, Double>();
		acctsHScores = new HashMap<String, LinkedHashSet<Health>>();
		acctFinances = new HashMap<String, LinkedHashSet<Finance>>();
		usersHScores = new HashMap<String, LinkedHashSet<Health>>();
		acctOppties = new HashMap<String, LinkedHashSet<Opportunity>>();
		acctTickets  = new HashMap<String, LinkedHashSet<Ticket>>();
		ChurnRenewalDates = new HashMap<String, AcctDates>();
		acctEntitlements = new HashMap<String, LinkedHashSet<Entitlement>>();
		acctInvoices = new HashMap<String, LinkedHashSet<Invoice>>();
		acctSubscriptions = new HashMap<String, LinkedHashSet<Subscription>>();
		
		acctSurveys = new HashMap<String, LinkedHashSet<Survey>>();
		acctCampaigns = new HashMap<String, LinkedHashSet<Campaign>>();		
		acctNotes = new HashMap<String, LinkedHashSet<Note>>();		
		acctInteractions = new HashMap<String, LinkedHashSet<Interaction>>();
		acctFeatureReqs = new HashMap<String, LinkedHashSet<FeatureReq>>();
		
	}
	
	@After
	public void tearDown() {

		if (AcctDates != null) {
			AcctDates.clear();
			AcctDates = null;
		}

		if (UserDates != null) {
			UserDates.clear();
			UserDates = null;
		}
		
		if (AcctMappping != null) {
			AcctMappping.clear();
			AcctMappping = null;
		}
		
		if (AcctScoring != null) {
			AcctScoring.clear();
			AcctScoring = null;
		}

		if (acctsHScores != null) {
			acctsHScores.clear();
			acctsHScores = null;
		}

		if (acctFinances != null) {
			acctFinances.clear();
			acctFinances = null;
		}

		if (usersHScores != null) {
			usersHScores.clear();
			usersHScores = null;
		}

		if (acctOppties != null) {
			acctOppties.clear();
			acctOppties = null;
		}

		if (acctTickets != null) {
			acctTickets.clear();
			acctTickets = null;
		}
		
		if (ChurnRenewalDates != null) {
			ChurnRenewalDates.clear();
			ChurnRenewalDates = null;
		}
		if (acctSurveys !=null) {
			acctSurveys.clear();
			acctSurveys = null;
		}
		if (acctCampaigns !=null) {
			acctCampaigns.clear();
			acctCampaigns = null;
		}
		if (acctNotes !=null) {
			acctNotes.clear();
			acctNotes = null;
		}
		if (acctInteractions !=null) {
			acctInteractions.clear();
			acctInteractions = null;
		}
		if (acctFeatureReqs !=null) {
			acctFeatureReqs.clear();
			acctFeatureReqs = null;
		}
		
		sFormat = null;
		usageDateFormat = null;
	}

	void insertGeneralAcctHealthScore(String srcFile, int type) throws IOException, ParseException {

		File sFile = new File(srcFile);
		BufferedReader br = new BufferedReader(new FileReader(sFile));

		String line = null;
		String[] splits = null;

		LinkedHashSet<Health> list = null;
		Health score = null;
		String acctId = null;
		int totalRecords = 0;
		int invalidRecords = 0;
		while ((line = br.readLine()) != null) {
			
			if (line.contains("mScore"))
				continue;
			totalRecords++;
			
			splits = line.split("\t");

			acctId = splits[3].toLowerCase().trim();
			if (type == CUSTOMER_TYPE.BNA_DEMO.ordinal()) {
				acctId = splits[BNA_CHI_SCORE.acctid.ordinal()].toLowerCase().trim();
			}
			
			list = acctsHScores.get(acctId);
			if (list == null) {
				list = new LinkedHashSet<Health>();
				acctsHScores.put(acctId, list);
			}
			score = new Health();
			
			if (type == CUSTOMER_TYPE.CLOUDPASSAGE.ordinal()) {
				int myScore = (int)Math.round(Double.parseDouble(splits[0]));
				score.setScore(myScore);
				score.setCreated(usageDateFormat.parse(splits[2]));
			}
			else if (type == CUSTOMER_TYPE.BRIGHTIDEA.ordinal()) {
				int myScore = (int)Math.round(Double.parseDouble(splits[0]));
				score.setScore(myScore);
				score.setCreated(usageDateFormat.parse(splits[2] + "01"));
			}
			else if (type == CUSTOMER_TYPE.BNA_DEMO.ordinal()) {
				Double percent15 = Double.parseDouble(splits[BNA_CHI_SCORE.percent15.ordinal()]);
				Double percent75 = Double.parseDouble(splits[BNA_CHI_SCORE.percent75.ordinal()]);
				Double myScore = Double.parseDouble(splits[BNA_CHI_SCORE.mscore.ordinal()]);
				score.setScore(myScore.compareTo(percent15) < 0? 30 : (myScore.compareTo(percent75) < 0? 70 : 90));
				score.setCreated(usageDateFormat.parse(splits[BNA_CHI_SCORE.month.ordinal()].replaceAll("-", "") + "01"));
				if ("880".equals(acctId)) {
					int test = 0;
				}
			}
			
			score.put("_id", score._id);
			score.put("created", score.created);
			score.put("score", score.score);
			score.put("scoreType", score.scoreType);
			
			list.add(score);
//			System.out.println(score);
		}

		br.close();
		
		System.out.printf("invalid scores %d\ttotal scores %d\n", invalidRecords, totalRecords);
	}

	
	void insertOpenAcctHealthScore(String srcFile) throws IOException, ParseException {

		File sFile = new File(srcFile);
		BufferedReader br = new BufferedReader(new FileReader(sFile));

		String line = null;
		// acctId  myScore month   free
		String[] splits = null;

		LinkedHashSet<Health> list = null;
		Health score = null;
		String acctId = null;
		int mScore = 0;
		int totalRecords = 0;
		int invalidRecords = 0;
		while ((line = br.readLine()) != null) {
			
			if (line.contains("myScore"))
				continue;
			totalRecords++;
			
			splits = line.split("\t");
			acctId = splits[0].toLowerCase().trim();
			
			list = acctsHScores.get(acctId);
			if (list == null) {
				list = new LinkedHashSet<Health>();
				acctsHScores.put(acctId, list);
			}
			mScore = Integer.parseInt(splits[1]);
			score = new Health();
			score.setScore(mScore);
			score.setCreated(sFormat.parse(splits[2] + "01"));
			
			score.put("_id", score._id);
			score.put("created", score.created);
			score.put("score", score.score);
			score.put("scoreType", score.scoreType);
			
			list.add(score);
//			System.out.println(score);
		}

		br.close();
		
		System.out.printf("invalid scores %d\ttotal scores %d\n", invalidRecords, totalRecords);
	}
	
	void insertAcctHealthScore(String srcFile) throws IOException, ParseException {

		File sFile = new File(srcFile);
		BufferedReader br = new BufferedReader(new FileReader(sFile));

		String line = null;
		String[] splits = null;

		LinkedHashSet<Health> list = null;
		Health score = null;
		String acctId = null;
		int totalRecords = 0;
		int invalidRecords = 0;
		while ((line = br.readLine()) != null) {
			
			if (line.contains("mScore"))
				continue;
			totalRecords++;
			
			splits = line.split("\t");
			
			// TODO:: last one
			acctId = splits[3].toLowerCase().trim();
			
			list = acctsHScores.get(acctId);
			if (list == null) {
				list = new LinkedHashSet<Health>();
				acctsHScores.put(acctId, list);
			}
			score = new Health();
			int myScore = (int)Math.round(Double.parseDouble(splits[0]));
			score.setScore(myScore);
			score.setCreated(sFormat.parse(splits[2] + "01"));
			
//			acctId = splits[1].toLowerCase().trim();
//			
//			list = acctsHScores.get(acctId);
//			if (list == null) {
//				list = new LinkedHashSet<Health>();
//				acctsHScores.put(acctId, list);
//			}
//			score = new Health();
//			int myScore = Integer.parseInt(splits[2]);
//			score.setScore(myScore);
//			score.setCreated(sFormat.parse(splits[0] + "01"));
			
//			acctId = splits[2].toLowerCase().trim();
//			
//			list = acctsHScores.get(acctId);
//			if (list == null) {
//				list = new LinkedHashSet<Health>();
//				acctsHScores.put(acctId, list);
//			}
//			score = new Health();
//			int myScore = (int)Math.round(Double.parseDouble(splits[0]));
//			score.setScore(myScore);
//			score.setCreated(sFormat.parse("20130901"));
			
			score.put("_id", score._id);
			score.put("created", score.created);
			score.put("score", score.score);
			score.put("scoreType", score.scoreType);
			
			list.add(score);
//			System.out.println(score);
		}

		br.close();
		
		System.out.printf("invalid scores %d\ttotal scores %d\n", invalidRecords, totalRecords);
	}


	void insertUserHealthScore(String srcFile) throws IOException, ParseException {

		File sFile = new File(srcFile);
		BufferedReader br = new BufferedReader(new FileReader(sFile));

		String line = br.readLine();
		String[] splits = null;
		LinkedHashSet<Health> list = null;
		Health score = null;
		String acctId = null;
		String userId = null;
		int totalRecords = 0;
		int invalidRecords = 0;
		while ((line = br.readLine()) != null) {
			
			if (line.contains("mScore"))
				continue;
			totalRecords++;
			
			splits = line.split("\t");
		
			userId = splits[1].trim();
			acctId = splits[2].trim();

			if (AcctMappping.get(acctId.toLowerCase()) == null) {
				invalidRecords++;
				continue;
			}
			
			userId = acctId + "-" + userId;
			userId = userId.toLowerCase();
			list = usersHScores.get(userId);
			if (list == null) {
				list = new LinkedHashSet<Health>();
				usersHScores.put(userId, list);
			}
			
			int myScore = Integer.parseInt(splits[0]);
			score = new Health();
			score.setScore((int)myScore);
			score.setCreated(sFormat.parse(splits[4] + "01"));

			score.put("_id", score._id);
			score.put("created", score.created);
			score.put("score", score.score);
			score.put("scoreType", score.scoreType);
			
//			System.out.println(score);
			
			list.add(score);
		}

		br.close();

		System.out.printf("invalid records %d\ttotal records %d\n", invalidRecords, totalRecords);

	}
	
	void insertTicketObject(String srcFile) throws Exception {

		File sFile = new File(srcFile);
		BufferedReader br = new BufferedReader(new FileReader(sFile));

		String line = null;


		LinkedHashSet<Ticket> list = null;
		String acctId = null;
		int totalRecords = 0;
		int invalidRecords = 0;
		while ((line = br.readLine()) != null) {
			
			if (line.contains("acctId"))
				continue;

			Ticket ticket = new Ticket();
			ticket.set(line, 3);

			acctId = ticket.getAcctId().toLowerCase();
			list = acctTickets.get(acctId);
			if (list == null) {
				list = new LinkedHashSet<Ticket>();
				acctTickets.put(acctId, list);
			}
			
			ticket.put("_id", ticket._id);
			ticket.put("bnaId", ticket.bnaId);
			ticket.put("assignee", ticket.getAssignee());
			ticket.put("status", ticket.getStatus());
			ticket.put("subject", ticket.getSubject());
			ticket.put("submitter", ticket.getSubmitter());
			ticket.put("creator", ticket.getCreator());
			ticket.put("created", ticket.getCreated());
			ticket.put("updated", ticket.getUpdated());
			ticket.put("resolvedDate", ticket.getResolvedDate());
			ticket.put("dueDate", ticket.getDue());
			ticket.put("resolved", ticket.isResolved());
			ticket.put("channel", ticket.getChannel());
			ticket.put("priority", ticket.getPriority());
			ticket.put("description", ticket.getDescription());
			
			list.add(ticket);
			
			totalRecords++;
		}

		br.close();
		
		System.out.printf("account tickets %d with invalid scores %d\ttotal records %d\n", acctTickets.size(), invalidRecords, totalRecords);
	}
	
	void insertOpportunityObject(String srcFile) throws Exception {

		File sFile = new File(srcFile);
		BufferedReader br = new BufferedReader(new FileReader(sFile));

		String line = null;

		LinkedHashSet<Opportunity> list = null;
		String acctId = null;
		int totalRecords = 0;
		int invalidRecords = 0;
		while ((line = br.readLine()) != null) {
			
			if (line.contains("mScore"))
				continue;
			totalRecords++;
			
			Opportunity oppty = new Opportunity();
			oppty.set(line, CUSTOMER_TYPE.BNA_DEMO.ordinal());

			acctId = oppty.getAcctId().toLowerCase();
			list = acctOppties.get(acctId);
			if (list == null) {
				list = new LinkedHashSet<Opportunity>();
				acctOppties.put(acctId, list);
			}
			
			oppty.put("_id", oppty._id);
			oppty.put("opptyId", oppty.getOpptyId());
			oppty.put("name", oppty.getName());
			if ("unknown".equals(oppty.getNextStep()) == false)
				oppty.put("nextStep", oppty.getNextStep());
			oppty.put("stage", oppty.getStage());
			oppty.put("owner", oppty.getOwner());
			oppty.put("probability", oppty.getProbability());
			oppty.put("leadSource", oppty.getLeadSource());
			oppty.put("externalService", oppty.getExternalService());
			if ("unknown".equals(oppty.getExternalServiceId()) == false)
				oppty.put("externalServiceId", oppty.getExternalServiceId());
			oppty.put("amount", oppty.getAmount());
			if ("cId".equals(oppty.getCampaignId()) == false)
				oppty.put("campaignId", oppty.getCampaignId());
			oppty.put("closeDate", oppty.getCloseDate());
			if (oppty.getLastActivity() != null)
				oppty.put("lastActivity", oppty.getLastActivity());
			oppty.put("created", oppty.getCreated());
			oppty.put("expectedRevenue", oppty.getExpectedRevenue());
			oppty.put("forecastCategory", oppty.getForecastCategory());
			oppty.put("closed", oppty.isClosed());
//			oppty.put("deleted", oppty.isDeleted());
			oppty.put("open", oppty.isOpen());
			if ("unknown".equals(oppty.getDescription()) == false)
				oppty.put("description", oppty.getDescription());
			if (oppty.getProduct() != null && "unknown".equals(oppty.getProduct()) == false)
				oppty.put("product", oppty.getProduct());
			
			list.add(oppty);
		}

		br.close();
		
		System.out.printf("account opportunities %d with invalid scores %d\ttotal scores %d\n", acctOppties.size(), invalidRecords, totalRecords);
	}
	
	void insertGeneralAccountObject(String srcFile, int custType) throws Exception {

		try {
			Mongo mongo = new Mongo(ipAddr, port);
			DB db = mongo.getDB(dbName);

//			MongoClient mongoClient = new MongoClient(ipAddr, port);
//			DB db = mongoClient.getDB(dbName);
//			boolean auth = db.authenticate(username, password.toCharArray());
//			if (!auth) {
//				System.out.println("authentication error!");
//				System.exit(1);	
//			}

			// TODO:: no enduser info
			DBCollection table = db.getCollection("endUser");
			table.drop();
			table = db.getCollection("endUser");
			
			table = db.getCollection("account");
			table.drop();
			table = db.getCollection("account");
			
			List<DBObject> feeds = new LinkedList<DBObject>();
			int threshold = 1000;

			ObjectId custId = null;
			if (custType == CUSTOMER_TYPE.BRIGHTIDEA.ordinal())
				custId = new ObjectId("5229f0663004e751ecdf8425");
			else if (custType == CUSTOMER_TYPE.CLOUDPASSAGE.ordinal())	
				custId = new ObjectId("5229f0663004e751ecdf8427");
			else if (custType == CUSTOMER_TYPE.BNA_DEMO.ordinal())	
				custId = new ObjectId("5229f0663004e751ecdf841c");
			
			File sFile = new File(srcFile);
			BufferedReader br = new BufferedReader(new FileReader(sFile));
			
			String line = null;		
			FirstLastEvent event = null;
			
			Set<String> acctSets = new HashSet<String>();
			while (threshold > 0 && (line = br.readLine()) != null) {
								
				if (line.contains("acctId"))
					continue;
				
				BasicDBObject mydbObject = new BasicDBObject();
				BNAacct acct = new BNAacct(line, custType);
				if (acctSets.contains(acct.getAcctId()))
					continue;
				else
					acctSets.add(acct.getAcctId());
				
				threshold--;
				mydbObject.put("customerId", custId);
				mydbObject.put("_id", acct.get_id());
				mydbObject.put("usageId", acct.getAcctId());
				if (acct.getArr() != null)
					mydbObject.put("arr", acct.getArr());
				if (acct.getMrr() != null)
					mydbObject.put("mrr", acct.getMrr());
				if (acct.getCsmName() != null)
					mydbObject.put("csmName", acct.getCsmName());
				if (acct.getSalesLead() != null)
					mydbObject.put("salesLead", acct.getSalesLead());
				if (acct.getEndUserCount() != null)
					mydbObject.put("endUserCount", acct.getEndUserCount());
				if (acct.getLocation() != null)
					mydbObject.put("location", acct.getLocation());
				if (acct.getName() != null)
					mydbObject.put("name", acct.getName());
//				mydbObject.put("region", acct.getRegion());
//				mydbObject.put("stage", acct.getStage());
//				mydbObject.put("tier", acct.getTier());		
//				mydbObject.put("supportLevel", acct.getSupportLevel());
				if (acct.getIndustry() != null)
					mydbObject.put("industry", acct.getIndustry());	
				if (acct.getSegment() != null)
					mydbObject.put("segment", acct.getSegment());	
//				mydbObject.put("sicCode", acct.getSicCode());
				if (acct.getContractedDT() != null)
					mydbObject.put("contractDate", usageDateFormat.parse(acct.getContractedDT().replaceAll("-", "")));
				if (acct.getRenewalDT() != null) // usageFullDateFormat
					mydbObject.put("renewalDate", "-9999".equals(acct.getRenewalDT())? null : usageDateFormat.parse(acct.getRenewalDT().replaceAll("-", "")));
				if (acct.getChurnDT() != null)
					mydbObject.put("churnDate", "-9999".equals(acct.getChurnDT())? null : usageDateFormat.parse(acct.getChurnDT().replaceAll("-", "")));
				if (acct.isChurn() != null)
					mydbObject.put("churn", acct.isChurn());	
				
				if (acct == null || acct.getAcctId() == null) {
					System.out.printf("no acct for data %s\n", line); 
					continue;
				}

				int a = 0;
				if (acct.getName().endsWith("Schulman"))
					a = -1;
				
				String dateStr = acct.getFirstDate();
				mydbObject.put("firstEvent", dateStr == null || dateStr.equals("-9999")? null : usageDateFormat.parse(dateStr.replaceAll("-", "")));
				dateStr = acct.getLastDate();
				if (dateStr != null && acct.isChurn() && usageDateFormat.parse(dateStr.replaceAll("-", "")).after(usageDateFormat.parse(acct.getChurnDT().replaceAll("-", ""))))
					dateStr = acct.getChurnDT().replaceAll("-", "");
				mydbObject.put("lastEvent", dateStr == null || dateStr.equals("-9999")? null : usageDateFormat.parse(dateStr.replaceAll("-", "")));
				
				// TODO:: general case
				event = AcctDates.get(acct.getAcctId().toLowerCase());
				if (event != null) {
					dateStr = event.getFirstDate();
					mydbObject.put("firstEvent", dateStr == null || dateStr.equals("-9999")? null : usageHourFormat.parse(dateStr));
					dateStr = event.getLastDate();
					mydbObject.put("lastEvent", dateStr == null || dateStr.equals("-9999")? null : usageHourFormat.parse(dateStr));
				}
				
				String acctId = acct.getAcctId().toLowerCase();
				LinkedHashSet<Health> hscores = acctsHScores.get(acctId);
				if (hscores == null || hscores.isEmpty()) {
					System.out.printf("no hscore for account %s\n", acctId);
				}
				else {
					// TODO:: filter out all CHI score if the date passed churn date
					LinkedHashSet<Health> modifiedScores = new LinkedHashSet<Health>();
					
					for (Health score : hscores) {
						if (acct.isChurn() && score.getCreated().before(usageDateFormat.parse(acct.getChurnDT().replaceAll("-", ""))))
							modifiedScores.add(score);
					}
					
					mydbObject.put("healthScores", modifiedScores);
				}
				LinkedHashSet<Ticket> tickets = acctTickets.get(acctId);
				if (tickets != null && tickets.isEmpty() == false)
					mydbObject.put("tickets", tickets);
				
				if (custType == CUSTOMER_TYPE.BNA_DEMO.ordinal()) {
					mydbObject.put("tickets", acctTickets.get("ticket"));
					mydbObject.put("opportunities", acctOppties.get("oppty"));
					mydbObject.put("entitlements", acctEntitlements.get("entitlement"));
					mydbObject.put("subscriptions", acctSubscriptions.get("subscription"));
					mydbObject.put("invoices", acctInvoices.get("invoice"));

					mydbObject.put("notes", acctNotes.get("note"));
					mydbObject.put("surveys", acctSurveys.get("survey"));
					mydbObject.put("campaigns", acctCampaigns.get("campaign"));
					mydbObject.put("featureRequests", acctFeatureReqs.get("feature"));
					mydbObject.put("interactions", acctInteractions.get("interaction"));
				
				}
				AcctMappping.put(acct.getAcctId().toLowerCase(), acct.get_id().toString());
								
				feeds.add(mydbObject);
				mydbObject = null;
				
				if (threshold == 0) {
					table.insert(feeds);
					threshold = 1000;
					feeds.clear();
					feeds = new LinkedList<DBObject>();
				}
			}
			acctSets.clear();
			acctSets = null;
			
			if (threshold > 0) {
				table.insert(feeds);
				feeds.clear();
				feeds = null;
			}
			
			br.close();

		} catch (UnknownHostException e) {
			e.printStackTrace(System.out);
		} catch (MongoException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	
	void insertOpenAccountObject(String srcFile) throws Exception {

		try {
			MongoClient mongoClient = new MongoClient(ipAddr, port);
			DB db = mongoClient.getDB(dbName);
			boolean auth = db.authenticate("bnaadmin", "bluenose!".toCharArray());

			DBCollection table = db.getCollection("account");
			table.drop();
			table = db.getCollection("account");
			
			List<DBObject> feeds = new LinkedList<DBObject>();
			int threshold = 1000;
			
			File sFile = new File(srcFile);
			BufferedReader br = new BufferedReader(new FileReader(sFile));
			
			ObjectId custId = new ObjectId("524c9ffbf7864895bdd8ee75");
			
			String line = null;
			String[] splits = null;
			while (threshold-- > 0 && (line = br.readLine()) != null) {

				if (line.contains("acctId"))
					continue;
				
				BasicDBObject mydbObject = new BasicDBObject();
				
				splits = line.split("\t");
				if (splits == null || splits.length != OPEN_ACCT.values().length)
					return;

				String acctId = splits[OPEN_ACCT.acctId.ordinal()].trim().toLowerCase();
				 
				mydbObject.put("customerId", custId); // openVPN
				mydbObject.put("_id", new ObjectId());
				mydbObject.put("name", acctId);	
				mydbObject.put("usageId", acctId);
				mydbObject.put("totalBandwidth", splits[OPEN_ACCT.tBandwidth.ordinal()].trim());
				double dVal = Double.parseDouble(splits[OPEN_ACCT.avgBandwidth.ordinal()].trim());
				mydbObject.put("avgBandwidth", dFormat.format(dVal));
				mydbObject.put("totalDuration", splits[OPEN_ACCT.tDuration.ordinal()].trim());
				dVal = Double.parseDouble(splits[OPEN_ACCT.avgDuration.ordinal()].trim());
				mydbObject.put("avgDuration", dFormat.format(dVal));
				mydbObject.put("firstEvent", new Date(Long.parseLong((splits[OPEN_ACCT.firstEvent.ordinal()].trim())) * 1000));
				mydbObject.put("lastEvent", new Date(Long.parseLong((splits[OPEN_ACCT.lastEvent.ordinal()].trim()))* 1000));
				dVal = Double.parseDouble(splits[OPEN_ACCT.livetime.ordinal()].trim());
				Long iVal = Math.round(dVal); 
				mydbObject.put("livetime", iVal);
				mydbObject.put("healthScores", acctsHScores.get(acctId));
				
				feeds.add(mydbObject);
				mydbObject = null;
				
				if (threshold == 0) {
					table.insert(feeds);
					threshold = 1000;
					feeds.clear();
					feeds = new LinkedList<DBObject>();
				}
			}
			if (threshold > 0) {
				table.insert(feeds);
				feeds.clear();
				feeds = null;
			}
			
			br.close();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	void insertBNAaccountObject(String srcFile, String custIdStr) throws Exception {

		try {
			Mongo mongo = new Mongo(ipAddr, port);
			DB db = mongo.getDB(dbName);

//			MongoClient mongoClient = new MongoClient(ipAddr, port);
//			DB db = mongoClient.getDB(dbName);
//			boolean auth = db.authenticate(username, password.toCharArray());
//			if (!auth) {
//				System.out.println("authentication error!");
//				System.exit(1);
//			}

			DBCollection table = db.getCollection("account");
			table.drop();
			table = db.getCollection("account");
			
			List<DBObject> feeds = new LinkedList<DBObject>();
			int threshold = 1000;

			ObjectId custId = new ObjectId(custIdStr);
			
			File sFile = new File(srcFile);
			BufferedReader br = new BufferedReader(new FileReader(sFile));
			
			String line = null;		
			FirstLastEvent event = null;
			LinkedHashSet<Finance> mrrList = null;
			Finance mrrObj = null;
			
			while (threshold > 0 && (line = br.readLine()) != null) {
			
				mrrObj = new Finance();
				String[] splits = line.split(CommonTokens.TAB_DELIMITER);
				mrrObj.setCreated(usageFullDateFormat.parse(splits[RELICON_ACCT.eventdt.ordinal()]));
				mrrObj.setValue((long)(Double.parseDouble(splits[RELICON_ACCT.mrr.ordinal()]) * 100));

				mrrObj.put("_id", mrrObj._id);
				mrrObj.put("created", mrrObj.created);
				mrrObj.put("value", mrrObj.value);
				mrrObj.put("currency", mrrObj.currency);
				mrrObj.put("unit", mrrObj.unit);
				
				mrrList = acctFinances.get(splits[RELICON_ACCT.acctId.ordinal()]);
				if (mrrList == null) {
					mrrList = new LinkedHashSet<Finance>();
					acctFinances.put(splits[RELICON_ACCT.acctId.ordinal()].trim().toLowerCase(), mrrList);
				}
				
				mrrList.add(mrrObj);
			}
			
			br.close();
			
			br = new BufferedReader(new FileReader(sFile));
			
			Set<String> acctSets = new HashSet<String>();
			while (threshold > 0 && (line = br.readLine()) != null) {
								
				BasicDBObject mydbObject = new BasicDBObject();
				BNAacct acct = new BNAacct(line, 2);
				if (acctSets.contains(acct.getAcctId()))
					continue;
				else
					acctSets.add(acct.getAcctId());
				
				threshold--;
				mydbObject.put("customerId", custId);
				mydbObject.put("_id", acct.get_id());
				mydbObject.put("usageId", acct.getAcctId());
//				mydbObject.put("arr", acct.getArr());
				mydbObject.put("mrr", acct.getMrr());
//				mydbObject.put("csmName", acct.getCsmName());
//				mydbObject.put("salesLead", acct.getSalesLead());
//				mydbObject.put("endUserCount", acct.getEndUserCount());
//				mydbObject.put("location", acct.getLocation());
				mydbObject.put("name", acct.getName());
//				mydbObject.put("region", acct.getRegion());
//				mydbObject.put("stage", acct.getStage());
//				mydbObject.put("tier", acct.getTier());		
//				mydbObject.put("supportLevel", acct.getSupportLevel());
//				mydbObject.put("industry", acct.getIndustry());		
				mydbObject.put("churn", acct.isChurn());	
				if (acct == null || acct.getAcctId() == null) {
					System.out.printf("no acct for data %s\n", line); 
					continue;
				}

				// TODO:: general case
				event = AcctDates.get(acct.getAcctId().toLowerCase());
				if (event == null) {
					System.out.printf("no event for acct: ID=%s\tName=%s\n", acct.getAcctId(), acct.getAcctName());
				}
				if (event != null) {
					mydbObject.put("firstEvent", usageMonthFormat.parse(event.getFirstDate()));
					mydbObject.put("lastEvent", usageMonthFormat.parse(event.getLastDate()));
				}
//				AcctDates dates = ChurnRenewalDates.get(acct.getAcctId());
//				if (dates == null) { 
//					dates = new AcctDates(null, null, null);
//					System.out.printf("no churn dates for acct: ID=%s\tName=%s\n", acct.getAcctId(), acct.getAcctName());
//				}
//				mydbObject.put("contractDate", dates.getStartDate());	
//				mydbObject.put("renewalDate", dates.getRenewalDate());
//				mydbObject.put("churnDate", dates.getChurnDate());
				// TODO:: for Replicon only
				mydbObject.put("contractDate", usageFullDateFormat.parse(acct.getContractedDT()));	
				mydbObject.put("renewalDate", "-9999".equals(acct.getRenewalDT())? null : usageFullDateFormat.parse(acct.getRenewalDT()));
				mydbObject.put("churnDate", "-9999".equals(acct.getChurnDT())? null : usageDateFormat.parse(acct.getChurnDT()));
				
				String acctId = acct.getAcctId().toLowerCase();
				LinkedHashSet<Health> hscores = acctsHScores.get(acctId);
				if (hscores == null) {
					System.out.printf("no hscore for account %s\n", acctId);
				}
				mydbObject.put("healthScores", hscores);
//				mydbObject.put("accountOpportunity", acctOppties.get(acctId));
				mrrList = acctFinances.get(acctId);
				mydbObject.put("mrrList", mrrList);
				
				AcctMappping.put(acct.getAcctId().toLowerCase(), acct.get_id().toString());
								
				feeds.add(mydbObject);
				mydbObject = null;
				
				if (threshold == 0) {
					table.insert(feeds);
					threshold = 1000;
					feeds.clear();
					feeds = new LinkedList<DBObject>();
				}
			}
			acctSets.clear();
			acctSets = null;
			
			if (threshold > 0) {
				table.insert(feeds);
				feeds.clear();
				feeds = null;
			}
			
			br.close();

		} catch (UnknownHostException e) {
			e.printStackTrace(System.out);
		} catch (MongoException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}
	
	static void loadAcctEvents(String acctDates) throws IOException {

		File sFile = new File(acctDates);
		BufferedReader br = new BufferedReader(new FileReader(sFile));
		
		String line = null;
		String[] splits = null; 
		
		while ((line = br.readLine()) != null) {
			if (line.contains("acctId"))
				continue;
			
			splits = line.split("\t");
			FirstLastEvent event = new FirstLastEvent(splits[0], splits[1]);
			AcctDates.put(splits[2].toLowerCase(), event);
		}
		
		br.close();
	}
	
	void loadEvents(int custType) throws IOException, ParseException {

		String acctDates = null;
		String userDates = null;
		String churnDates = null;
		String subscriptions = null;
		String tickets = null;
		String entitlements = null;
		String invoices = null;
		String opportunities = null;

		String survey = null;
		String campaigns = null;
		String notes = null;
		String interactions = null;
		String featureReq = null;
		
		if (custType == CUSTOMER_TYPE.BNA_DEMO.ordinal()) {
			acctDates = "/Users/borongzhou/test/fake/product/acctFirstLastDate.tsv";
			userDates = "/Users/borongzhou/test/fake/product/userFirstLastDate.tsv";
			churnDates = "/Users/borongzhou/test/fake/product/acctDates.tsv";
			
			subscriptions = "/Users/borongzhou/test/fake/product/subscription.tsv";
			tickets = "/Users/borongzhou/test/fake/product/tickets.tsv";
			entitlements = "/Users/borongzhou/test/fake/product/entitlement.tsv";
			invoices = "/Users/borongzhou/test/fake/product/invoices.tsv";
			opportunities = "/Users/borongzhou/test/fake/product/oppty.tsv";
			survey = "/Users/borongzhou/test/fake/product/survey.tsv";
			campaigns = "/Users/borongzhou/test/fake/product/campaigns.tsv";
			notes = "/Users/borongzhou/test/fake/product/notes.tsv";
			interactions = "/Users/borongzhou/test/fake/product/interactions.tsv";
			featureReq = "/Users/borongzhou/test/fake/product/featureReq.tsv";
		}
		else if (custType == CUSTOMER_TYPE.REPLICON.ordinal()) {
			acctDates = "/Users/borongzhou/test/replicon/product/acctFirstLastDate.tsv";
			userDates = "/Users/borongzhou/test/replicon/product/userFirstLastDate.tsv";
//			churnDates = "/Users/borongzhou/test/replicon/product/acctDates.tsv";
		}
		
		File sFile = null;
		BufferedReader br = null;

		String line = null;
		String[] splits = null; 
		
		if (custType != CUSTOMER_TYPE.BNA_DEMO.ordinal()) {
			sFile = new File(acctDates);
			br = new BufferedReader(new FileReader(sFile));
			while ((line = br.readLine()) != null) {
				splits = line.split("\t");
				FirstLastEvent event = new FirstLastEvent(splits[0], splits[1]);
				AcctDates.put(splits[2].toLowerCase(), event);
			}

			br.close();

			sFile = new File(userDates);
			br = new BufferedReader(new FileReader(sFile));

			line = null;
			splits = null;

			while ((line = br.readLine()) != null) {
				splits = line.split("\t");
				FirstLastEvent event = new FirstLastEvent(splits[0], splits[1]);
				UserDates.put(splits[2] + "\t" + splits[3], event);
			}

			br.close();

			sFile = new File(churnDates);
			br = new BufferedReader(new FileReader(sFile));

			line = null;
			splits = null;
			while ((line = br.readLine()) != null) {
				splits = line.split("\t");
				if (splits.length != 4) {
					System.out.printf("invalid record: %s\n", line);
					continue;
				}

				AcctDates event = new AcctDates(splits[1],
						"-9999".equals(splits[2]) ? null : splits[2],
						"-9999".equals(splits[3]) ? null : splits[3]);
				ChurnRenewalDates.put(splits[0], event);
			}
			br.close();
		}
		
		// acctId, startDate, renewalDate, churnDate
		// TODO:: for BNA demo only???
		if (custType == CUSTOMER_TYPE.BNA_DEMO.ordinal()) {	
			sFile = new File(subscriptions);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<Subscription> sList = null;
			while ((line = br.readLine()) != null) {
				Subscription myObj = new Subscription();
				myObj.set(line, custType);
				sList = acctSubscriptions.get("subscription");
				if (sList == null) {
					sList = new LinkedHashSet<Subscription>();
					acctSubscriptions.put("subscription", sList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("activated", myObj.getActivated());
				myObj.put("currency", myObj.getCurrency());
				myObj.put("current_period_ends_at", myObj.getCurrent_period_ends_at());
				myObj.put("current_period_started_at", myObj.getCurrent_period_started_at());
				myObj.put("name", myObj.getName());
				myObj.put("quantity", myObj.getQuantity());
				myObj.put("state", myObj.getState());
				
				sList.add(myObj);
			}
			
			br.close();
			
			sFile = new File(entitlements);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<Entitlement> eList = null;
			while ((line = br.readLine()) != null) {
				Entitlement myObj = new Entitlement();
				myObj.set(line, custType);
				eList = acctEntitlements.get("entitlement");
				if (eList == null) {
					eList = new LinkedHashSet<Entitlement>();
					acctEntitlements.put("entitlement", eList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("description", myObj.getDescription());
				myObj.put("name", myObj.getName());
				myObj.put("productId", myObj.getProductId());
				myObj.put("startDate", myObj.getStartDate());
				myObj.put("status", myObj.getStatus());
				myObj.put("type", myObj.getType());
				
				eList.add(myObj);
			}
			
			br.close();

			sFile = new File(invoices);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<Invoice> iList = null;
			while ((line = br.readLine()) != null) {
				Invoice myObj = new Invoice();
				myObj.set(line, custType);
				iList = acctInvoices.get("invoice");
				if (iList == null) {
					iList = new LinkedHashSet<Invoice>();
					acctInvoices.put("invoice", iList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("created", myObj.getCreated());
				myObj.put("currency", myObj.getCurrency());
				myObj.put("paidDate", myObj.getPaidDate());
				myObj.put("poNumber", myObj.getPoNumber());
				myObj.put("state", myObj.getState());
				myObj.put("total", myObj.getTotal());
				
				iList.add(myObj);
			}
			
			br.close();

			sFile = new File(tickets);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<Ticket> tList = null;
			while ((line = br.readLine()) != null) {
				Ticket myObj = new Ticket();
				myObj.set(line, custType);
				tList = acctTickets.get("ticket");
				if (tList == null) {
					tList = new LinkedHashSet<Ticket>();
					acctTickets.put("ticket", tList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("created", myObj.getCreated());
				myObj.put("status", myObj.getStatus());
				myObj.put("subject", myObj.getSubject());
				myObj.put("priority", myObj.getPriority());
				myObj.put("assignee", myObj.getAssignee());
				myObj.put("resolvedDate", myObj.getResolvedDate());
				
				tList.add(myObj);
			}
			
			br.close();
			sFile = new File(opportunities);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<Opportunity> pList = null;
			while ((line = br.readLine()) != null) {
				Opportunity myObj = new Opportunity();
				myObj.set(line, custType);
				pList = acctOppties.get("oppty");
				if (pList == null) {
					pList = new LinkedHashSet<Opportunity>();
					acctOppties.put("oppty", pList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("name", myObj.getName());
				myObj.put("stage", myObj.getStage());
				myObj.put("probability", myObj.getProbability());
				myObj.put("amount", myObj.getAmount());
				myObj.put("expectedRevenue", myObj.getExpectedRevenue());
				myObj.put("closeDate", myObj.getCloseDate());
				
				pList.add(myObj);
			}
			
			br.close();
			
			sFile = new File(survey);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<Survey> suvList = null;
			while ((line = br.readLine()) != null) {
				Survey myObj = new Survey();
				myObj.set(line, custType);
				suvList = acctSurveys.get("survey");
				if (suvList == null) {
					suvList = new LinkedHashSet<Survey>();
					acctSurveys.put("survey", suvList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("name", myObj.getName());
				myObj.put("endUser", myObj.getEndUser());
				myObj.put("value", myObj.getValue());
				myObj.put("createdDate", myObj.getCreatedDate());
				
				suvList.add(myObj);
			}
			
			br.close();

			sFile = new File(featureReq);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<FeatureReq> fetList = null;
			while ((line = br.readLine()) != null) {
				FeatureReq myObj = new FeatureReq();
				myObj.set(line, custType);
				fetList = acctFeatureReqs.get("feature");
				if (fetList == null) {
					fetList = new LinkedHashSet<FeatureReq>();
					acctFeatureReqs.put("feature", fetList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("assignee", myObj.getAssignee());
				myObj.put("created", myObj.getCreated());
				myObj.put("status", myObj.getStatus());
				myObj.put("subject", myObj.getSubject());
				myObj.put("priority", myObj.getPriority());
				myObj.put("resolvedDate", myObj.getResolvedDate());
				
				fetList.add(myObj);
			}
			
			br.close();

			sFile = new File(interactions);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<Interaction> intList = null;
			while ((line = br.readLine()) != null) {
				Interaction myObj = new Interaction();
				myObj.set(line, custType);
				intList = acctInteractions.get("interaction");
				if (intList == null) {
					intList = new LinkedHashSet<Interaction>();
					acctInteractions.put("interaction", intList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("date", myObj.getDate());
				myObj.put("endUserName", myObj.getEndUserName());
				myObj.put("type", myObj.getType());
				myObj.put("creator", myObj.getCreator());
				
				intList.add(myObj);
			}
			
			br.close();

			sFile = new File(notes);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<Note> nList = null;
			while ((line = br.readLine()) != null) {
				Note myObj = new Note();
				myObj.set(line, custType);
				nList = acctNotes.get("note");
				if (nList == null) {
					nList = new LinkedHashSet<Note>();
					acctNotes.put("note", nList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("created", myObj.getCreated());
				myObj.put("title", myObj.getTitle());
				myObj.put("creator", myObj.getCreator());
				
				nList.add(myObj);
			}
			
			br.close();

			sFile = new File(campaigns);
			br = new BufferedReader(new FileReader(sFile));

			line = br.readLine();
			LinkedHashSet<Campaign> cList = null;
			while ((line = br.readLine()) != null) {
				Campaign myObj = new Campaign();
				myObj.set(line, custType);
				cList = acctCampaigns.get("campaign");
				if (cList == null) {
					cList = new LinkedHashSet<Campaign>();
					acctCampaigns.put("campaign", cList);
				}
				
				myObj.put("_id", myObj._id);
				myObj.put("date", myObj.getDate());
				myObj.put("open", myObj.getOpen());
				myObj.put("name", myObj.getName());
				myObj.put("recipient", myObj.getRecipient());
				
				cList.add(myObj);
			}
			
			br.close();
		}
	}

	void insertBNAenduserObject(String srcFile, int type) throws Exception {

		try {
			Mongo mongo = new Mongo(ipAddr, port);
			DB db = mongo.getDB(dbName);

//			MongoClient mongoClient = new MongoClient(ipAddr, port);
//			DB db = mongoClient.getDB(dbName);
//			boolean auth = db.authenticate(username, password.toCharArray());
//			if (!auth) {
//				System.out.println("authentication error!");
//				System.exit(1);
//			}
			
			DBCollection table = db.getCollection("endUser");
			table.drop();
			table = db.getCollection("endUser");
			
			List<DBObject> feeds = new LinkedList<DBObject>();
			int threshold = 1000;
			
			File sFile = new File(srcFile); // "/Users/borongzhou/test/replicon/product/endUserFromUsage.tsv");
			BufferedReader br = new BufferedReader(new FileReader(sFile));
			
			String line = null;
			
//			while((line = br.readLine()) != null) {
//				String[] splits = line.split("\t");
//				
//				if ("521093".equals(splits[0])) {	
//					System.out.printf("there is the data %s for 521093 with fist=%s\t2nd=%s\n", line,splits[0],splits[1]);
//				}
//			}
//			
//			br.close();
//			
//			sFile = new File("/Users/borongzhou/test/replicon/product/endUserFromUsage.tsv");
//			br = new BufferedReader(new FileReader(sFile));
			
			line = null;
			int total = 0;
			int invalid = 0;
			FirstLastEvent event = null;
			while ((line = br.readLine()) != null) {
				
				String[] splits = line.split("\t");
				
				if ("521093".equals(splits[0])) {
					System.out.printf("inserting data for account %s\n", line);
				}
				BasicDBObject mydbObject = new BasicDBObject();
				BNAendUser ensuser = new BNAendUser(line, type);

				total++;
				if (ensuser.getAccountId() == null) {
					invalid++;
					continue;
				}
				
				mydbObject.put("_id", ensuser.get_id());
				mydbObject.put("accountId", ensuser.getAccountId());
				mydbObject.put("name", ensuser.getUserId());	
				mydbObject.put("firstEvent", ensuser.getFirstDT() == null? null : usageMonthFormat.parse(ensuser.getFirstDT()));
				mydbObject.put("lastEvent", ensuser.getLastDT() == null? null : usageMonthFormat.parse(ensuser.getLastDT()));
				
//				event = UserDates.get(ensuser.getUserId());
//				if (event == null) {
//					System.out.printf("no event for user: Name=%s\n", ensuser.getUserId());
//					continue;
//					
//				}
//				mydbObject.put("firstEvent", usageDateFormat.parse(event.getFirstDate()));
//				mydbObject.put("lastEvent", usageDateFormat.parse(event.getLastDate()));
				String userId = ensuser.getAcctId() + "-" + ensuser.getUserId();
//				mydbObject.put("healthScores", usersHScores.get(userId.toLowerCase()));
				
				feeds.add(mydbObject);
				mydbObject = null;
				threshold--;
				
				if (threshold == 0) {
					table.insert(feeds);
					threshold = 1000;
					feeds.clear();
					feeds = new LinkedList<DBObject>();
				}
			}
			if (threshold > 0 && feeds.isEmpty() == false) {
				table.insert(feeds);
				feeds.clear();
				feeds = null;
			}
			
			br.close();
			
			System.out.printf("Enduser, total=%d\tinvalid=%d\n ", total, invalid);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	String toJson(String line) throws Exception {
		return new BNAacct(line, 0).toString();
	}
	
	String toJson2(String line) throws Exception {
		return new UsageCount(line).toString();
	}

	public static class FeatureReq extends BasicDBObject {
		private ObjectId _id = new ObjectId();
		private Date created;
		private String status;
		private String subject;
		private Integer priority;
		private String assignee;
		private Date resolvedDate;

		public void set(String data, int type) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (splits.length != 6)
				return;
			
			this.created = StringUtil.isNullOrEmpty(splits[0]) || "-".equals(splits[0])? null : sFormat.parse(splits[0]);
			this.status = splits[1];
			this.subject = splits[2];
			this.priority = StringUtil.isNullOrEmpty(splits[3]) || "-".equals(splits[3])? null : Integer.parseInt(splits[3]);
			this.assignee = splits[4];
			this.resolvedDate = StringUtil.isNullOrEmpty(splits[5]) || "-".equals(splits[5])? null : sFormat.parse(splits[5]);
		}

		public ObjectId get_id() {
			return _id;
		}
		public Date getCreated() {
			return created;
		}
		public String getStatus() {
			return status;
		}
		public String getSubject() {
			return subject;
		}
		public Integer getPriority() {
			return priority;
		}
		public String getAssignee() {
			return assignee;
		}
		public Date getResolvedDate() {
			return resolvedDate;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
	}

	
	public static class Interaction extends BasicDBObject {
		private ObjectId _id = new ObjectId();
		private Date date;
		private String endUserName;
		private String type;
		private String creator;

		public void set(String data, int type) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (splits.length != 4)
				return;
			
			this.date = StringUtil.isNullOrEmpty(splits[0]) || "-".equals(splits[0])? null : sFormat.parse(splits[0]);
			this.endUserName = splits[1];
			this.type = splits[2];
			this.creator = splits[3];
		}

		public ObjectId get_id() {
			return _id;
		}
		public String getCreator() {
			return creator;
		}
		public String getEndUserName() {
			return endUserName;
		}
		public String getType() {
			return type;
		}
		public Date getDate() {
			return date;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
	}

	
	public static class Note extends BasicDBObject {
		private ObjectId _id = new ObjectId();
		private Date created;
		private String title;
		private String creator;

		public void set(String data, int type) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (splits.length != 3)
				return;
			
			this.created = StringUtil.isNullOrEmpty(splits[0]) || "-".equals(splits[0])? null : sFormat.parse(splits[0]);
			this.title = splits[1];
			this.creator = splits[2];
		}

		public ObjectId get_id() {
			return _id;
		}
		public String getTitle() {
			return title;
		}
		public String getCreator() {
			return creator;
		}
		public Date getCreated() {
			return created;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
	}

	
	public static class Campaign extends BasicDBObject {
		private ObjectId _id = new ObjectId();
		private String name;
		private String recipient;
		private Boolean open;
		private Date date;

		public void set(String data, int type) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (splits.length != 4)
				return;
			
			this.date = StringUtil.isNullOrEmpty(splits[0]) || "-".equals(splits[0])? null : sFormat.parse(splits[0]);
			this.name = splits[1];
			this.recipient = splits[2];
			this.open = StringUtil.isNullOrEmpty(splits[3]) || "-".equals(splits[0])? false : Boolean.parseBoolean(splits[3]);
		}

		public ObjectId get_id() {
			return _id;
		}
		public String getName() {
			return name;
		}
		public String getRecipient() {
			return recipient;
		}
		public Boolean getOpen() {
			return open;
		}
		public Date getDate() {
			return date;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
	}

	
	public static class Survey extends BasicDBObject {
		private ObjectId _id = new ObjectId();
		private String name;
		private String endUser;
		private String value;
		private Date createdDate;

		public void set(String data, int type) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (splits.length != 4)
				return;
			
			this.createdDate = StringUtil.isNullOrEmpty(splits[0]) || "-".equals(splits[0])? null : sFormat.parse(splits[0]);
			this.name = splits[1];
			this.endUser = splits[2];
			this.value = splits[3];
		}

		public ObjectId get_id() {
			return _id;
		}
		public String getName() {
			return name;
		}
		public String getEndUser() {
			return endUser;
		}
		public String getValue() {
			return value;
		}
		public Date getCreatedDate() {
			return createdDate;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
	}

	
	public static class Entitlement extends BasicDBObject {
		
		private ObjectId _id = new ObjectId();
		private String name;
		private String description;
		private String type;
		private String status;
		private String productId;
		private Date startDate;

		public void set(String data, int type) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (splits.length != 6)
				return;
			
			this.name = splits[0];
			this.description = splits[1];
			this.type = splits[2];
			this.status = splits[3];
			this.productId = splits[4];
			this.startDate = StringUtil.isNullOrEmpty(splits[5]) || "-".equals(splits[5])? null : sFormat.parse(splits[5]);
		}

		public ObjectId get_id() {
			return _id;
		}
		public String getName() {
			return name;
		}
		public String getDescription() {
			return description;
		}
		public String getType() {
			return type;
		}
		public String getStatus() {
			return status;
		}
		public String getProductId() {
			return productId;
		}
		public Date getStartDate() {
			return startDate;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
	}
	
	public static class Invoice extends BasicDBObject {
		
		private ObjectId _id = new ObjectId();
		private String state;
		private Integer invoiceNumber;
		private Long poNumber;
		private String vatNumber;
		private Long total;
		private String currency;
		private Date paidDate;
		private Date created;
		
		public void set(String data, int type) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (splits.length != 8)
				return;
			
			this.state = splits[0];
			this.invoiceNumber = Integer.parseInt(splits[1]);
			this.poNumber = StringUtil.isNullOrEmpty(splits[2])? null : Long.parseLong(splits[2]);
			this.vatNumber = splits[3];
			this.total = StringUtil.isNullOrEmpty(splits[4])? null : Long.parseLong(splits[4]);
			this.currency = splits[5];
			this.paidDate = StringUtil.isNullOrEmpty(splits[6]) || "-".equals(splits[6])? null : sFormat.parse(splits[6]);
			this.created = StringUtil.isNullOrEmpty(splits[7]) || "-".equals(splits[7])? null : sFormat.parse(splits[7]);
		}
		
		public ObjectId get_id() {
			return _id;
		}
		public String getState() {
			return state;
		}
		public Integer getInvoiceNumber() {
			return invoiceNumber;
		}
		public Long getPoNumber() {
			return poNumber;
		}
		public String getVatNumber() {
			return vatNumber;
		}
		public Long getTotal() {
			return total;
		}
		public String getCurrency() {
			return currency;
		}
		public Date getPaidDate() {
			return paidDate;
		}
		public Date getCreated() {
			return created;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
	}
	
	public static class Subscription extends BasicDBObject {

		private ObjectId _id = new ObjectId();
		private String planCode;
		private String name;
		private String state;
		private Long unitAmount;
		private String currency;
		private Integer quantity;
		private Date activated;
		private Date current_period_started_at;
		private Date current_period_ends_at;

		public void set(String data, int type) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (splits.length != 9)
				return;
			
			this.planCode = splits[0];
			this.name = splits[1];
			this.state = splits[2];
			this.unitAmount = StringUtil.isNullOrEmpty(splits[3])? null : Long.parseLong(splits[3]);
			this.currency = splits[4];
			this.quantity = StringUtil.isNullOrEmpty(splits[5])? null : Integer.parseInt(splits[5]);
			this.activated = StringUtil.isNullOrEmpty(splits[6]) || "-".equals(splits[6])? null : sFormat.parse(splits[6]);
			this.current_period_started_at = StringUtil.isNullOrEmpty(splits[7]) || "-".equals(splits[7])? null : sFormat.parse(splits[7]);
			this.current_period_ends_at = StringUtil.isNullOrEmpty(splits[8]) || "-".equals(splits[8])? null : sFormat.parse(splits[8]);
		}

		public ObjectId get_id() {
			return _id;
		}
		public String getPlanCode() {
			return planCode;
		}
		public String getName() {
			return name;
		}
		public String getState() {
			return state;
		}
		public Long getUnitAmount() {
			return unitAmount;
		}
		public String getCurrency() {
			return currency;
		}
		public Integer getQuantity() {
			return quantity;
		}
		public Date getActivated() {
			return activated;
		}
		public Date getCurrent_period_started_at() {
			return current_period_started_at;
		}
		public Date getCurrent_period_ends_at() {
			return current_period_ends_at;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
	}
	
	public static class Ticket extends BasicDBObject {
		private ObjectId _id = new ObjectId();
		private String bnaId = null;

		private ObjectId accountId = null;
		private String acctId = null;
		private String externalId = null;
		private String type = null;
		private String subject = null;
		private String description = null;
		private String priority = null;
		private String status = null;
		private String submitter = null;
		private String recipient = null;
		private String assignee = null;
		private Date due = null;
		private Date created = null;
		private Date updated = null;
		private String creator = null;  //requester

		private boolean closed = false;
		private Date closedDate = null;
		private boolean resolved = false;
		private Date resolvedDate = null;
		private String 	product = null;
		private String component = null;
		private String reason = null;
		private String channel = null;
		private boolean deleted = false;

		public void set(String data, int type) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String str = null;
			String[] splits = data.split("\t");

			if (type == CUSTOMER_TYPE.BNA_DEMO.ordinal()) {
				if (splits.length != 6)
					return;
				
				this.created = StringUtil.isNullOrEmpty(splits[0])? null : sFormat.parse(splits[0]);
				this.status = splits[1];
				this.subject = splits[2];
				this.priority = splits[3];
				this.assignee = splits[4];
				this.resolvedDate = StringUtil.isNullOrEmpty(splits[5]) || "-".equals(splits[5])? null : sFormat.parse(splits[5]);
			}
			else if (type == CUSTOMER_TYPE.BRIGHTIDEA.ordinal()) {
				
				if (splits.length != BRIGHT_TICKET.values().length)
						return;
				
				this.bnaId = splits[BRIGHT_TICKET.niceId.ordinal()];
				this.acctId = splits[BRIGHT_TICKET.acctId.ordinal()];
				this.assignee = splits[BRIGHT_TICKET.assignedId.ordinal()];
				this.status = splits[BRIGHT_TICKET.statusId.ordinal()];
				this.subject = splits[BRIGHT_TICKET.subject.ordinal()];
				this.submitter = splits[BRIGHT_TICKET.submitterId.ordinal()];
				this.creator = splits[BRIGHT_TICKET.requesterId.ordinal()];
				str = splits[BRIGHT_TICKET.createdDT.ordinal()];
				if (StringUtil.isNullOrEmpty(str) == false)
					this.created = ticketDateFormat.parse(str);
				str = splits[BRIGHT_TICKET.updatedDT.ordinal()];
				if (StringUtil.isNullOrEmpty(str) == false)
					this.updated  = ticketDateFormat.parse(str);
				str = splits[BRIGHT_TICKET.solvedDT.ordinal()];
				if (StringUtil.isNullOrEmpty(str) == false)
					this.resolvedDate = ticketDateFormat.parse(str);
				str = splits[BRIGHT_TICKET.dueDT.ordinal()];
				if (StringUtil.isNullOrEmpty(str) == false)
					this.due = ticketDateFormat.parse(str);
				this.resolved = Boolean.parseBoolean(splits[BRIGHT_TICKET.solved.ordinal()]);
				this.channel = splits[BRIGHT_TICKET.channelid.ordinal()];
				this.priority = splits[BRIGHT_TICKET.priorityId.ordinal()];
				this.description = splits[BRIGHT_TICKET.currentTag.ordinal()]; // original desc is too much
			}

		}
		
		public ObjectId get_id() {
			return _id;
		}
		public String getBnaId() {
			return bnaId;
		}
		public void setAccountId(ObjectId accountId) {
			this.accountId = accountId;
		}
		public ObjectId getAccountId() {
			return accountId;
		}
		public String getAcctId() {
			return acctId;
		}
		public String getExternalId() {
			return externalId;
		}
		public String getType() {
			return type;
		}
		public String getSubject() {
			return subject;
		}
		public String getDescription() {
			return description;
		}
		public String getPriority() {
			return priority;
		}
		public String getStatus() {
			return status;
		}
		public String getRecipient() {
			return recipient;
		}
		public String getSubmitter() {
			return submitter;
		}
		public String getAssignee() {
			return assignee;
		}
		public Date getDue() {
			return due;
		}
		public Date getCreated() {
			return created;
		}
		public Date getUpdated() {
			return updated;
		}
		public String getCreator() {
			return creator;
		}
		public boolean isClosed() {
			return closed;
		}
		public Date getClosedDate() {
			return closedDate;
		}
		public boolean isResolved() {
			return resolved;
		}
		public Date getResolvedDate() {
			return resolvedDate;
		}
		public String getProduct() {
			return product;
		}
		public String getComponent() {
			return component;
		}
		public String getReason() {
			return reason;
		}
		public String getChannel() {
			return channel;
		}
		public boolean isDeleted() {
			return deleted;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}

	}

	public static class Opportunity extends BasicDBObject {
		
		ObjectId _id = null;
		String acctId = null;
		String opptyId = null;
		String name = null;
		String nextStep = null;
		String stage = null;
		String owner = null;
		String probability = null;
		String leadSource = null;
		String externalService = null;
		String externalServiceId = null;
		String amount = null;
		String campaignId = null;
		Date closeDate = null;
		Date lastActivity = null;
		Date created = null;
		String description = null;
		String expectedRevenue = null;
		String forecastCategory = null;
		String product = null;
		boolean closed = false;
		boolean deleted = false;
		boolean open = false;
		
		public void set(String data, int custType) throws ParseException {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (custType == CUSTOMER_TYPE.BNA_DEMO.ordinal()) {
				if (splits.length != 6)
					return;
				
				this.name = splits[0];
				this.stage = splits[1];
				this.probability = splits[2];
				this.amount = splits[3];
				this.expectedRevenue = splits[4];
				this.closeDate = StringUtil.isNullOrEmpty(splits[5])? null : sFormat.parse(splits[5]);
			}
			else {
			if (splits.length != HOST_OPPTY.values().length)
				return;
			
			this.acctId = splits[HOST_OPPTY.acctId.ordinal()];
			this.opptyId = splits[HOST_OPPTY.opptyId.ordinal()];
			this.name = splits[HOST_OPPTY.opptyName.ordinal()];
			this.nextStep = splits[HOST_OPPTY.nextStep.ordinal()];
			this.stage = splits[HOST_OPPTY.stage.ordinal()];
			this.owner = splits[HOST_OPPTY.opptyOwner.ordinal()];
			this.probability = splits[HOST_OPPTY.probability.ordinal()];
			this.leadSource = splits[HOST_OPPTY.leadSource.ordinal()];
			this.externalService = splits[HOST_OPPTY.externalService.ordinal()];
			this.externalServiceId = splits[HOST_OPPTY.externalServiceId.ordinal()];
			this.amount = splits[HOST_OPPTY.amount.ordinal()];
			this.campaignId = splits[HOST_OPPTY.campaignId.ordinal()];
			this.product = splits[HOST_OPPTY.opptyProducts.ordinal()];
					
			String str = splits[HOST_OPPTY.closeDate.ordinal()];
			try {
				this.closeDate = "-9999".equals(str)? null : sFormat.parse(str);
			} catch (ParseException e) {
				this.closeDate = null;
			}
			str = splits[HOST_OPPTY.lastActivity.ordinal()];
			try {
				this.lastActivity = "-9999".equals(str)? null : sFormat.parse(str);
			} catch (ParseException e) {
				this.lastActivity = null;
			}
			str = splits[HOST_OPPTY.createdDate.ordinal()];
			try {
				this.created = "-9999".equals(str)? null : sFormat.parse(str);
			} catch (ParseException e) {
				this.created = null;
			}
			
			this.description = splits[HOST_OPPTY.description.ordinal()];
			this.expectedRevenue = splits[HOST_OPPTY.expectedRevenue.ordinal()];
			this.forecastCategory = splits[HOST_OPPTY.forecastCategory.ordinal()];
			this.closed = Boolean.parseBoolean(splits[HOST_OPPTY.closed.ordinal()]);
			this.deleted = Boolean.parseBoolean(splits[HOST_OPPTY.deleted.ordinal()]);
			this.open = Boolean.parseBoolean(splits[HOST_OPPTY.open.ordinal()]);
			}
			
			this._id = new ObjectId();
		}
		
		public ObjectId get_id() {
			return _id;
		}
		public String getAcctId() {
			return acctId;
		}
		public String getOpptyId() {
			return opptyId;
		}
		public String getName() {
			return name;
		}
		public String getNextStep() {
			return nextStep;
		}
		public String getStage() {
			return stage;
		}
		public String getOwner() {
			return owner;
		}
		public String getProbability() {
			return probability;
		}
		public String getLeadSource() {
			return leadSource;
		}
		public String getExternalService() {
			return externalService;
		}
		public String getExternalServiceId() {
			return externalServiceId;
		}
		public String getAmount() {
			return amount;
		}
		public String getCampaignId() {
			return campaignId;
		}
		public Date getCloseDate() {
			return closeDate;
		}
		public Date getLastActivity() {
			return lastActivity;
		}
		public Date getCreated() {
			return created;
		}
		public String getDescription() {
			return description;
		}
		public String getExpectedRevenue() {
			return expectedRevenue;
		}
		public String getForecastCategory() {
			return forecastCategory;
		}
		public boolean isClosed() {
			return closed;
		}
		public boolean isDeleted() {
			return deleted;
		}
		public boolean isOpen() {
			return open;
		}
		public String getProduct() {
			return product;
		}

		@Override
		public String toString() {
			return new Gson().toJson(this);
		}
	}
	

	/**
	 * by default the value is in cents, and $
	 * @author borongzhou
	 *
	 */
	public static class Finance extends BasicDBObject {
		
		public ObjectId _id = new ObjectId();
		public String name = "MRR";
		public String currency = "USD";
		public String unit = "cent";
		public long value = 0L;
		public Date created = null;
		public String description = null;
		
		public ObjectId get_id() {
			return _id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getCurrency() {
			return currency;
		}
		public void setCurrency(String currency) {
			this.currency = currency;
		}
		public String getUnit() {
			return unit;
		}
		public void setUnit(String unit) {
			this.unit = unit;
		}
		public long getValue() {
			return value;
		}
		public void setValue(long value) {
			this.value = value;
		}
		public Date getCreated() {
			return created;
		}
		public void setCreated(Date created) {
			this.created = created;
		}
		public String getDescription() {
			return description;
		}
		public void setDescription(String description) {
			this.description = description;
		}

		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
		
	}
	
	public static class Health extends BasicDBObject {
		
		public ObjectId _id = new ObjectId();
		public Date created = null;
		public int score = 0;
		public String scoreType = "auto";
		
		public ObjectId get_id() {
			return _id;
		}
		public Date getCreated() {
			return created;
		}
		public void setCreated(Date created) {
			this.created = created;
		}
		public int getScore() {
			return score;
		}
		public void setScore(int score) {
			this.score = score;
		}
		public String getScoreType() {
			return scoreType;
		}
		public void setScoreType(String scoreType) {
			this.scoreType = scoreType;
		}
		
		@Override
		public String toString() {
			
			return GSON.toJson(this);
		}
	}

	public static class AcctDates {
		
		Date startDate = null;
		Date renewalDate = null;
		Date churnDate = null;
		
		public AcctDates(String startStr, String renewalStr, String churnStr) {
			
			try {
				if (startStr != null)
					this.startDate = sFormat.parse(startStr);
				if (renewalStr != null)
					this.renewalDate = sFormat.parse(renewalStr);
				if (churnStr != null)
					this.churnDate = sFormat.parse(churnStr);
			} catch(ParseException ex) {
				
			}
		}
		
		public Date getStartDate() {
			return startDate;
		}
		public Date getRenewalDate() {
			return renewalDate;
		}
		public Date getChurnDate() {
			return churnDate;
		}
		
	}
	
	public static class FirstLastEvent {
		
		String firstDate = null;
		String lastDate = null;
		
		public FirstLastEvent(String firstDate, String lastDate) {
			this.firstDate = firstDate;
			this.lastDate = lastDate;
		}
		
		public String getFirstDate() {
			return firstDate;
		}
		public String getLastDate() {
			return lastDate;
		}
		
	}

	public static class BNAendUser {
		private Object _id = null;
		private Object accountId = null;
		private String acctId = null;
		private String userId = null;
		private String firstDT = null;
		private String lastDT = null;
		
		public BNAendUser(String data, int type) {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			
			if (type != CUSTOMER_TYPE.BNA_DEMO.ordinal()) {
				if (splits.length != 2)
					return;

				this.userId = splits[1];

				String acctId = splits[0].toLowerCase().trim();
				this.acctId = acctId;
				if ("521093".equals(acctId))
					System.out.printf("inserting enduser %s\n", data);

				if (AcctMappping.get(acctId) == null) {
					System.out.println("no objectId for " + acctId);
				} else
					this.accountId = new ObjectId(AcctMappping.get(acctId));

				FirstLastEvent dates = UserDates.get(splits[0] + "\t"
						+ splits[1]);
				if (dates == null)
					System.out.printf("no dates for user %s\n", splits[0]
							+ "\t" + splits[1]);
				this.firstDT = (dates == null) ? null : dates.firstDate;
				this.lastDT = (dates == null) ? null : dates.lastDate;
			}
			else {
				if (splits.length != BNA_USAGE.values().length)
					return;
				
				this.userId = splits[BNA_USAGE.username.ordinal()];
				String acctId = splits[BNA_USAGE.acctid.ordinal()].toLowerCase().trim();
				this.acctId = acctId;
				if (AcctMappping.get(acctId) == null) {
					System.out.println("no objectId for " + acctId);
				}
				else
					this.accountId = new ObjectId(AcctMappping.get(acctId));
				
				this.firstDT = splits[BNA_USAGE.firstDate.ordinal()];
				this.lastDT = splits[BNA_USAGE.firstDate.ordinal()];
			}
		}

		public Object get_id() {
			return _id;
		}
		public Object getAccountId() {
			return accountId;
		}
		public String getAcctId() {
			return acctId;
		}

		public String getUserId() {
			return userId;
		}
		public String getFirstDT() {
			return firstDT;
		}
		public String getLastDT() {
			return lastDT;
		}

	}
	
	public static class BNAcust {
		private ObjectId _id = new ObjectId();

		public ObjectId get_id() {
			return _id;
		}
	}
	
	public static class BNAacct {
		private ObjectId _id = null;
		private String acctId = null;
		private String acctName = null;
		private String csmName = null;
		private String salesLead = null;
		private String tier = null;
		private String location = null;
		private String region = null;
		private String supportLevel = null;
		private Integer endUserCount = null;
		private Long arr = null;
		private Long mrr = null;
		private String stage = null;
		private String segment = null;
		private String industry = null;
		private String contractedDT = null;
		private String created = null;
		private String renewalDT = null;
		private String churnDT = null;
		private Boolean churn = null;

		private Integer endUserCountUp = null;
		private String gORl = null;
		private String sso = null;
		private String sicCode = null;
		private String m2m = "Y";
		private String acctHealth = null;
		private String firstDate = null;
		private String lastDate = null;
		
		public BNAacct(String data, int type) throws ParseException, Exception {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");

			if (type == CUSTOMER_TYPE.BRIGHTIDEA.ordinal()) {
				if (splits.length != BRIGHT_ACCT.values().length)
					return;

				this.acctId = splits[BRIGHT_ACCT.acctId.ordinal()].trim();
				this.acctName = splits[BRIGHT_ACCT.acctName.ordinal()].trim();
				this.csmName = splits[BRIGHT_ACCT.csmName.ordinal()].trim();
				this.salesLead = splits[BRIGHT_ACCT.salesLead.ordinal()];
				this.endUserCount = Integer.parseInt(splits[BRIGHT_ACCT.numOfEmp
						.ordinal()]);
				this.endUserCountUp = Integer.parseInt(splits[BRIGHT_ACCT.numOfEmpUp
						.ordinal()]);
				double arrDouble = Double.parseDouble(splits[BRIGHT_ACCT.arr
						.ordinal()]);
				this.arr = (long) (arrDouble * 100);
				this.industry = splits[BRIGHT_ACCT.industry.ordinal()];
				this.segment = splits[BRIGHT_ACCT.segment.ordinal()];
				this.contractedDT = splits[BRIGHT_ACCT.contractedDT.ordinal()];
				this.churnDT = splits[BRIGHT_ACCT.churnDT.ordinal()];
				this.renewalDT = splits[BRIGHT_ACCT.renewalDT.ordinal()];
				this.churn = Boolean.parseBoolean(splits[BRIGHT_ACCT.churn
						.ordinal()]);
			}
			else if (type == CUSTOMER_TYPE.CLOUDPASSAGE.ordinal()) {
				if (splits.length != CLOUDPAS_ACCT.values().length)
					return;

				this.acctId = splits[CLOUDPAS_ACCT.acctId.ordinal()].trim();
				this.acctName = splits[CLOUDPAS_ACCT.acctName.ordinal()].trim();
				this.contractedDT = splits[CLOUDPAS_ACCT.contractedDT.ordinal()];
			}
			else if (type == CUSTOMER_TYPE.REPLICON.ordinal()) {
				if (splits.length != RELICON_ACCT.values().length)
					return;

				this.acctId = splits[RELICON_ACCT.acctId.ordinal()].trim();
				this.acctName = splits[RELICON_ACCT.acctName.ordinal()].trim();
				double mrrDouble = Double.parseDouble(splits[RELICON_ACCT.mrr
						.ordinal()]);
				this.mrr = (long) (mrrDouble * 100);
				this.contractedDT = splits[RELICON_ACCT.contractedDT.ordinal()];
				this.renewalDT = splits[RELICON_ACCT.renewalDT.ordinal()];
				this.m2m = splits[RELICON_ACCT.m2m.ordinal()];

				// TODO:: adjust the churn date here if Y + 30; or + 365 days
				this.churnDT = splits[RELICON_ACCT.churnDT.ordinal()];
				this.churn = Boolean.parseBoolean(splits[RELICON_ACCT.churn
						.ordinal()]);
			}
			// BNA fake
			else if (type == CUSTOMER_TYPE.BNA_DEMO.ordinal()) {
				if (splits.length != BNA_ACCT.values().length)
					return;

				this.acctId = splits[BNA_ACCT.acctId.ordinal()].trim();
				this.acctName = splits[BNA_ACCT.acctName.ordinal()].trim();

				if (acctName != null && acctName.startsWith("\""))
					acctName = acctName.substring(1);
				acctName = acctName != null && acctName.endsWith("\"")? acctName.substring(0, acctName.length() - 1) : acctName;
				acctName = acctName != null && acctName.contains("\"\"")?acctName.replaceAll("\"\"", "'") : acctName;

				this.csmName = splits[BNA_ACCT.csmName.ordinal()].trim();
				this.salesLead = splits[BNA_ACCT.salesLead.ordinal()];
				this.tier = splits[BNA_ACCT.tier.ordinal()];
				this.location = splits[BNA_ACCT.state.ordinal()];
				this.region = splits[BNA_ACCT.region.ordinal()];
				this.supportLevel = splits[BNA_ACCT.supportLevel.ordinal()];
				this.endUserCount = Integer.parseInt(splits[BNA_ACCT.numOfEmp
						.ordinal()]);
				double arrDouble = Double.parseDouble(splits[BNA_ACCT.arr
						.ordinal()]);
				this.arr = (long) (arrDouble * 100);
				this.stage = splits[BNA_ACCT.stage.ordinal()];
				this.contractedDT = splits[BNA_ACCT.contractedDT.ordinal()];
				this.renewalDT = splits[BNA_ACCT.renewalDT.ordinal()];
				this.churnDT = splits[BNA_ACCT.churnDT.ordinal()];
				this.churn = "-9999".equals(splits[BNA_ACCT.churnDT.ordinal()])? false : true;
				this.firstDate = splits[BNA_ACCT.firstDT.ordinal()];
				this.lastDate = splits[BNA_ACCT.lastDT.ordinal()];
			}
			
			this._id = new ObjectId();
		}
		
		public ObjectId get_id() {
			return _id;
		}
		public String getAcctId() {
			return acctId;
		}
		public String getAcctName() {
			return acctName;
		}
		public String getName() {
			return acctName;
		}
		public String getCsmName() {
			return csmName;
		}
		public String getSalesLead() {
			return salesLead;
		}
		public String getLocation() {
			return location;
		}
		public String getRegion() {
			return region;
		}
		public Integer getEndUserCount() {
			return endUserCount;
		}
		public Long getMrr() {
			return mrr;
		}
		public Long getArr() {
			return arr;
		}
		public String getTier() {
			return tier;
		}
		public String getStage() {
			return stage;
		}
		public String getIndustry() {
			return industry;
		}
		public String getGORl() {
			return gORl;
		}
		public Boolean isChurn() {
			return churn;
		}
		public String getSupportLevel() {
			return supportLevel;
		}
		public String getContractedDT() {
			return contractedDT;
		}
		public String getCreated() {
			return created;
		}
		public String getRenewalDT() {
			return renewalDT;
		}
		public String getChurnDT() {
			return churnDT;
		}
		public String getSegment() {
			return segment;
		}
		public String getSicCode() {
			return sicCode;
		}
		public String getgORl() {
			return gORl;
		}
		public String getFirstDate() {
			return firstDate;
		}
		public String getLastDate() {
			return lastDate;
		}

		@Override
		public String toString() {
			return GSON.toJson(this);
		}
	}

	static enum CUSTOMER_TYPE {
		BNA_DEMO,    // 0
		BNA_SIMULATOR,
		REPLICON,    // 2
		BRIGHTIDEA,
		CLOUDPASSAGE // 4
	}
	static enum BRIGHT_TICKET {
		  niceId,
		  acctName,
		  acctId,
		  subject,
		  baseScore,
		  score,
		  statusId,
		  requesterId,
		  submitterId,
		  assignedId,
		  group_id,
		  orgId,
		  createdDT,
		  updatedDT,
		  assignedDT,
		  dueDT,
		  solvedDT,
		  resolutionDT,
		  hasInceidents,
		  channelid,
		  priorityId,
		  currentTag,
		  solved
	}
	
	static enum BRIGHT_ACCT {
		acctId,
		acctName,
		acctHealth,
		arr,
		salesLead,
		csmName,
		status,
		rating,
		numOfEmp,
		numOfEmpUp,
		segment,
		sicCode,
		ipmGeo,
		industry,
		suspended,
		sso,
		tsPurchased,
		createdDT,
		contractedDT,
		renewalDT,
		churnDT,
		churn
	}
	
	static enum HOST_OPPTY {
		acctId,
		opptyId,
		opptyName,
		nextStep,
		stage,
		opptyOwner,
		probability,
		leadSource,
		externalService,
		externalServiceId,
		amount,
		campaignId,
		closeDate,
		lastActivity,
		createdDate,
		expectedRevenue,
		forecastCategory,
		closed,
		deleted,
		open,
		opptyProducts,
		description
	}

	static enum OPEN_ACCT {
		acctId,
		tBandwidth,
		tDuration,
		avgBandwidth,
		avgDuration,
		firstEvent,  // TODO:: convert date format yyyyMMdd
		lastEvent,
		livetime     // TODO:: convert integer
	}

	static enum BNA_ACCT_OLD {
		  acctId,
		  acctName,
		  csmName,
		  salesLead,
		  tier,
		  state,
		  region,
		  numOfEmp,
		  arr,
		  contractedDT,
		  supportLevel,
		  stage,
		  renewalDT,
		  churn
	}
	
	static enum BNA_CHI_SCORE {
		mscore,
		percent15,
		percent75,
		acctid,
		customerid,
		month
	}
	
	static enum BNA_USAGE {
		firstDate,
		lastDate,
		acctid,
		userid,
		username
	}
	
	static enum BNA_ACCT {
		  acctId,
		  acctName,
		  csmName,
		  salesLead,
		  tier,
		  state,
		  region,
		  supportLevel,
		  numOfEmp,
		  arr,
		  stage,
		  dhour,
		  mday,
		  yweek,
		  ymonth,
		  quarter,
		  yyear,
		  contractedDT,
		  contractedts,
		  renewalDT,
		  churnDT,
		  firstDT,
		  lastDT,
		  datekey,
		  custid
	}
	
	static enum HOST_ACCT_V3 {
		acctId,
		acctName,
		csmName,
		salesLead,
		tier,
		state,
		region,
		supportLevel,
//		numEmp,
//		annRev,
//		mrr,
		stage,
//		gORl,
//		industry,
		renewalDT,
		firstDT,
		lastDT,
		churn
	}
	
	static enum CLOUDPAS_ACCT {
		acctId,
		acctName,
		contractedDT
	}
	
	static enum RELICON_ACCT {
		acctId,
		acctName,
		mrr,
		m2m,
		eventdt,
		contractedDT,
		churnDT,
		renewalDT,
		churn
	}
	
	static enum HOST_ACCT_V2 {		
	   acctId,
	   csmName,
	   salesLead,
	   acctName,
	   state,
	   tier,
	   supportLevel,
	   numEmp,
	   annRev,
	   product,
	   gORl,
	   industry,
	   churn
	}
	
	public static class UsageCount {
		private int theCount = 0;
		private long theYear = 0L;
		private short theHour = 0;
		
		public UsageCount(String data) {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			if (splits.length != 3)
				return;
			
			this.theCount = Integer.parseInt(splits[0]);
			this.theYear = Long.parseLong(splits[1]);
			this.theHour = Short.parseShort(splits[2]);
		}
		
		public int getTheCount() {
			return theCount;
		}
		public void setTheCount(int theCount) {
			this.theCount = theCount;
		}
		public long getTheYear() {
			return theYear;
		}
		public void setTheYear(long theYear) {
			this.theYear = theYear;
		}
		public short getTheHour() {
			return theHour;
		}
		public void setTheHour(short theHour) {
			this.theHour = theHour;
		}
		
		@Override
		public String toString() {
			return GSON.toJson(this);
		}
	}
}