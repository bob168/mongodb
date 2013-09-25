package com.demo.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
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

	private static final String ipAddr = "10.0.9.11"; //"ec2-23-22-156-75.compute-1.amazonaws.com";
	private static final String dbName = "bow-fa8e12345678900000000002"; // "bow-fa8e12345678900000000000";
	
	private Map<String, LinkedHashSet<Health>> acctsHScores = null;
	private Map<String, LinkedHashSet<Health>> usersHScores = null;
	private static SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMdd");
	private static SimpleDateFormat usageDateFormat = new SimpleDateFormat("yyyyMMdd"); //"yyyy-MM-dd hh:mm:ss.SSS");
	private static final Gson GSON = new Gson();

	@Test
	public void insert()  {
		
		try {
			loadEvents();
			insertAcctHealthScore("/Users/borongzhou/test/hostAnalysis/product2/hostAcctHealthScore.tsv");
			insertBNAaccountObject("/Users/borongzhou/test/hostAnalysis/product2/acctsFromAccts3.tsv"); // "/Users/borongzhou/test/hostAnalysis/acctsFromAcctOutMRR.tsv");
			acctsHScores.clear();
			acctsHScores = null;		
		
			insertUserHealthScore("/Users/borongzhou/test/hostAnalysis/product2/hostUserHealthScore.tsv");
			insertBNAenduserObject("/Users/borongzhou/test/hostAnalysis/product2/usageEnduser3.tsv");

			usersHScores.clear();
			usersHScores = null;
		} catch(Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	@Before
	public void setup() {
		AcctDates = new HashMap<String, FirstLastEvent>();
		UserDates = new HashMap<String, FirstLastEvent>();
		AcctMappping = new HashMap<String, String>();
		AcctScoring = new HashMap<String, Double>();
		acctsHScores = new HashMap<String, LinkedHashSet<Health>>();
		usersHScores = new HashMap<String, LinkedHashSet<Health>>();
		ChurnRenewalDates = new HashMap<String, AcctDates>();
	}
	
	@After
	public void teranDown() {

		AcctDates.clear();
		AcctDates = null;

		UserDates.clear();
		UserDates = null;
		
		AcctMappping.clear();
		AcctMappping = null;
		
		AcctScoring.clear();
		AcctScoring = null;

		if (acctsHScores != null) {
			acctsHScores.clear();
			acctsHScores = null;
		}

		if (usersHScores != null) {
			usersHScores.clear();
			usersHScores = null;
		}
		
		ChurnRenewalDates.clear();
		ChurnRenewalDates = null;
		
		sFormat = null;
		usageDateFormat = null;
	}
	
	void insertAcctHealthScore(String srcFile) throws IOException, ParseException {

		File sFile = new File(srcFile);
		BufferedReader br = new BufferedReader(new FileReader(sFile));

		String line = null;
		String[] splits = null;

		LinkedHashSet<Health> list = null;
		Health score = null;
		String acctId = null;
		double maxScore = 0.0;
		double mScore = 0.0;
		int totalRecords = 0;
		int invalidRecords = 0;
		while ((line = br.readLine()) != null) {
			
			if (line.contains("mScore"))
				continue;
			totalRecords++;
			
			splits = line.split("\t");
			acctId = splits[2].toLowerCase().trim();
			
			list = acctsHScores.get(acctId);
			if (list == null) {
				list = new LinkedHashSet<Health>();
				acctsHScores.put(acctId, list);
			}
			mScore = Double.parseDouble(splits[0]);
			maxScore = Double.parseDouble(splits[1]);
			score = new Health();
			long myScore = Math.round(100*mScore / maxScore);
			if (myScore > 100)
				System.out.printf("mScore=%f, maxScore=%f, myScore=%d\n", mScore, maxScore, myScore);
			score.setScore(String.valueOf(myScore));
			score.setCreated(sFormat.parse(splits[4] + "01"));
			
			score.put("_id", score._id);
			score.put("created", score.created);
			score.put("score", score.score);
			score.put("scoreType", score.scoreType);
			
			list.add(score);
//			System.out.println(score);
		}

		br.close();
		
		System.out.printf("max Score %d with invalid scores %d\ttotal scores %d\n", Math.round(maxScore), invalidRecords, totalRecords);
	}


	void insertUserHealthScore(String srcFile) throws IOException, ParseException {

		File sFile = new File(srcFile);
		BufferedReader br = new BufferedReader(new FileReader(sFile));

		String line = br.readLine();
		String[] splits = null;
		LinkedHashSet<Health> list = null;
		Health score = null;
		Double mScore = 0.0;
		Double maxScore = 0.0;
		String acctId = null;
		String userId = null;
		int totalRecords = 0;
		int invalidRecords = 0;
		while ((line = br.readLine()) != null) {
			
			if (line.contains("mScore"))
				continue;
			totalRecords++;
			
			splits = line.split("\t");
			mScore = Double.parseDouble(splits[0]);
			maxScore = Double.parseDouble(splits[1]);
		
			userId = splits[2].trim();
			acctId = splits[3].trim();

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
			
			long myScore = Math.round(100.0*mScore / maxScore);
			if (myScore > 100)
				System.out.printf("mScore=%f, maxScore=%f, myScore=%d\n", mScore, maxScore, myScore);
			
			score = new Health();
			score.setScore(String.valueOf(myScore));
			score.setCreated(sFormat.parse(splits[5] + "01"));

			score.put("_id", score._id);
			score.put("created", score.created);
			score.put("score", score.score);
			score.put("scoreType", score.scoreType);
			
//			System.out.println(score);
			
			list.add(score);
		}

		br.close();

		System.out.printf("max Score %d with invalid records %d\ttotal records %d\n", Math.round(maxScore), invalidRecords, totalRecords);

	}


	@Test
	public void retrieve() {

		try {
			Mongo mongo = new Mongo(ipAddr, 27017);
			DB db = mongo.getDB(dbName);

			DBCollection user = db.getCollection("endUser");
			
			BasicDBObject whereQuery = new BasicDBObject();
//			whereQuery.put("name", "vamsi krishna"); 
//			whereQuery.put("accountId", new ObjectId("52051655e4b0b995b2e11c62")); 
			DBCursor cursor = user.find(whereQuery);

			System.out.println(user.count());
			
			int count = 10;
			while (count-- > 0 && cursor.hasNext()) {
				System.out.println(cursor.next());
			}
			cursor.close();
			
			DBCollection account = db.getCollection("account");
			whereQuery = new BasicDBObject();
//			whereQuery.put("name", "mirion technologies inc");
//			whereQuery.put("_id", new ObjectId("52051655e4b0b995b2e11c62"));
			
			DBCursor cursorDoc = account.find(whereQuery);
			count = 10;
			while (count--> 0 && cursorDoc.hasNext()) {
				System.out.println(cursorDoc.next());
			}

			System.out.println(account.count());
			cursorDoc.close();
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	public static class Health extends BasicDBObject {
		
		public ObjectId _id = new ObjectId();
		public Date created = null;
		public String score = "0";
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
		public String getScore() {
			return score;
		}
		public void setScore(String score) {
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
	
	void insertBNAaccountObject(String srcFile) throws Exception {

		try {
			Mongo mongo = new Mongo(ipAddr, 27017);
			DB db = mongo.getDB(dbName);

			DBCollection table = db.getCollection("account");
			table.drop();
			table = db.getCollection("account");
			
			List<DBObject> feeds = new LinkedList<DBObject>();
			int threshold = 1000;
			
			File sFile = new File(srcFile);
			BufferedReader br = new BufferedReader(new FileReader(sFile));
			
			String line = null;		
			FirstLastEvent event = null;
			while (threshold-- > 0 && (line = br.readLine()) != null) {

				BasicDBObject mydbObject = new BasicDBObject();
				BNAacct acct = new BNAacct(line);
				mydbObject.put("_id", acct.get_id());
				mydbObject.put("usageId", acct.getAcctId());
				mydbObject.put("arr", acct.getArr());
				mydbObject.put("mrr", acct.getMrr());
				mydbObject.put("csmName", acct.getCsmName());
				mydbObject.put("salesLead", acct.getSalesLead());
				mydbObject.put("endUserCount", acct.getEndUserCount());
				mydbObject.put("location", acct.getLocation());
				mydbObject.put("name", acct.getName());
				mydbObject.put("region", acct.getRegion());
				mydbObject.put("stage", acct.getStage());
				mydbObject.put("tier", acct.getTier());		
				mydbObject.put("supportLevel", acct.getSupportLevel());
				mydbObject.put("industry", acct.getIndustry());	
				mydbObject.put("gORl", acct.getGORl());				
				mydbObject.put("churn", acct.isChurn());				
				event = AcctDates.get(acct.getAcctId().toLowerCase());
				if (event == null) {
					System.out.printf("no event for acct: ID=%s\tName=%s\n", acct.getAcctId(), acct.getAcctName());
				}
				if (event != null) {
					mydbObject.put("firstEvent", usageDateFormat.parse(event.getFirstDate()));
					mydbObject.put("lastEvent", usageDateFormat.parse(event.getLastDate()));
				}
				AcctDates dates = ChurnRenewalDates.get(acct.getAcctId());
				if (dates == null) { 
					dates = new AcctDates(null, null, null);
					System.out.printf("no churn dates for acct: ID=%s\tName=%s\n", acct.getAcctId(), acct.getAcctName());
				}
				mydbObject.put("contractDate", dates.getStartDate());				
				mydbObject.put("renewalDate", dates.getRenewalDate());
				mydbObject.put("churnDate", dates.getChurnDate());
				
				mydbObject.put("healthScores", acctsHScores.get(acct.getAcctId().toLowerCase()));

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

	public static void main(String[] args)  {
		
		// 1377414001;1377414018;24
		System.out.println(new Date(1377414001));
		System.out.println(new Date(1377414013));
//		1377414001;1377414013;12
		System.out.println((1377414013 - 1377414001));
		
//		try {
//			Mongo mongo = new Mongo("localhost", 27017);
//			DB db = mongo.getDB("bow");
//
//			DBCollection collection = db.getCollection("hosting");
//			collection.drop();
//			collection = db.getCollection("hosting");
//
//			// 2. BasicDBObjectBuilder example
//			System.out.println("BasicDBObjectBuilder example...");
//			BasicDBObjectBuilder documentBuilder = BasicDBObjectBuilder.start()
//					.add("database", "bow").add("table", "hosting");
//
//			BasicDBObjectBuilder documentBuilderDetail = BasicDBObjectBuilder
//					.start().add("_id", new ObjectId()).add("records", "99")
//					.add("index", "vps_index1").add("active", "true");
//
//			documentBuilder.add("detail", documentBuilderDetail.get());
//
//			collection.insert(documentBuilder.get());
//
//			DBCursor cursorDocBuilder = collection.find();
//			while (cursorDocBuilder.hasNext()) {
//				System.out.println(cursorDocBuilder.next());
//			}
//
//			collection.remove(new BasicDBObject());
//			
//			
//			// 3. Map example
//			System.out.println("Map example...");
//			Map<String, Object> documentMap = new HashMap<String, Object>();
//			documentMap.put("database", "bow");
//			documentMap.put("table", "hosting");
//
//			Map<String, Object> documentMapDetail = new HashMap<String, Object>();
//			documentMapDetail.put("records", "99");
//			documentMapDetail.put("index", "vps_index1");
//			documentMapDetail.put("active", "true");
//
//			documentMap.put("detail", documentMapDetail);
//
//			collection.insert(new BasicDBObject(documentMap));
//
//			DBCursor cursorDocMap = collection.find();
//			while (cursorDocMap.hasNext()) {
//				System.out.println(cursorDocMap.next());
//			}
//
//			collection.remove(new BasicDBObject());
//
//		} catch(Throwable ex) {
//			ex.printStackTrace(System.out);
//		}
	}
		
	static void loadEvents() throws IOException {

		String acctDates = "/Users/borongzhou/test/hostAnalysis/product2/acctFirstLastDate3.tsv";
		String userDates = "/Users/borongzhou/test/hostAnalysis/product2/userFirstLastDate3.tsv";
		String churnDates = "/Users/borongzhou/test/hostAnalysis/product2/acctDates3.tsv";
		
		File sFile = new File(acctDates);
		BufferedReader br = new BufferedReader(new FileReader(sFile));
		
		String line = null;
		String[] splits = null; 
		
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
			UserDates.put(splits[2], event);
		}
		
		br.close();
		
		// acctId, startDate, renewalDate, churnDate
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
			
			AcctDates event = new AcctDates(splits[1], "-9999".equals(splits[2])? null : splits[2], "-9999".equals(splits[3])? null : splits[3]);
			ChurnRenewalDates.put(splits[0], event);
		}
		
		br.close();
	}

	void insertBNAenduserObject(String srcFile) throws Exception {

		try {
			Mongo mongo = new Mongo(ipAddr, 27017);
			DB db = mongo.getDB(dbName);

			DBCollection table = db.getCollection("endUser");
			table.drop();
			table = db.getCollection("endUser");
			
			List<DBObject> feeds = new LinkedList<DBObject>();
			int threshold = 1000;
			
			File sFile = new File(srcFile);
			BufferedReader br = new BufferedReader(new FileReader(sFile));
			
			String line = null;
			int total = 0;
			int invalid = 0;
			FirstLastEvent event = null;
			while (threshold-- > 0 && (line = br.readLine()) != null) {

				BasicDBObject mydbObject = new BasicDBObject();
				BNAendUser ensuser = new BNAendUser(line);

				total++;
				if (ensuser.getAccountId() == null) {
					invalid++;
					continue;
				}
				
				mydbObject.put("_id", ensuser.get_id());
				mydbObject.put("accountId", ensuser.getAccountId());
				mydbObject.put("name", ensuser.getUserId());	

				event = UserDates.get(ensuser.getUserId());
				if (event == null) {
					System.out.printf("no event for user: Name=%s\n", ensuser.getUserId());
					continue;
					
				}
				mydbObject.put("firstEvent", usageDateFormat.parse(event.getFirstDate()));
				mydbObject.put("lastEvent", usageDateFormat.parse(event.getLastDate()));
				String userId = ensuser.getAcctId() + "-" + ensuser.getUserId();
				mydbObject.put("healthScores", usersHScores.get(userId.toLowerCase()));
				
				feeds.add(mydbObject);
				mydbObject = null;
				
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
		return new BNAacct(line).toString();
	}
	
	String toJson2(String line) throws Exception {
		return new UsageCount(line).toString();
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
				//
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
		
		public BNAendUser(String data) {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			if (splits.length != 4)
				return;
			
			this.userId = splits[2];
			this._id = new ObjectId();

			String acctId = splits[0].toLowerCase().trim();
			this.acctId = acctId;
			if (AcctMappping.get(acctId) == null) {
				System.out.println("no objectId for " + acctId);
			}
			else
				this.accountId = new ObjectId(AcctMappping.get(acctId));
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

	}
		  
	public static class BNAacct {
		private Object _id = null;
		private String acctId = null;
		private String acctName = null;
		private String csmName = null;
		private String salesLead = null;
		private String tier = null;
		private String location = null;
		private String region = null;
		private String supportLevel = null;
		private String endUserCount = null;
		private String arr = null;
		private String mrr = null;
		private String stage = null;
		private String gORl = null;
		private String industry = null;
		private boolean churn = false;
		
		public BNAacct(String data) {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			if (splits.length != HOST_ACCT_V3.values().length)
				return;
			
			this.acctId = splits[HOST_ACCT_V3.acctId.ordinal()].trim();
			this.acctName = splits[HOST_ACCT_V3.acctName.ordinal()].trim();
			this.csmName = splits[HOST_ACCT_V3.csmName.ordinal()].trim();
			this.salesLead = splits[HOST_ACCT_V3.salesLead.ordinal()];
			this.tier = splits[HOST_ACCT_V3.tier.ordinal()];
			this.location = splits[HOST_ACCT_V3.state.ordinal()];
			this.region = splits[HOST_ACCT_V3.region.ordinal()];
			this.supportLevel = splits[HOST_ACCT_V3.supportLevel.ordinal()];
			this.endUserCount = splits[HOST_ACCT_V3.numEmp.ordinal()];
			this.arr = splits[HOST_ACCT_V3.annRev.ordinal()];
			this.mrr = splits[HOST_ACCT_V3.mrr.ordinal()];
			this.stage = splits[HOST_ACCT_V3.stage.ordinal()];
			this.gORl = splits[HOST_ACCT_V3.gORl.ordinal()];
			this.industry = splits[HOST_ACCT_V3.industry.ordinal()];
			this.churn = Boolean.parseBoolean(splits[HOST_ACCT_V3.churn.ordinal()]);
			
			this._id = new ObjectId();
		}
		
		public Object get_id() {
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
		public String getEndUserCount() {
			return endUserCount;
		}
		public String getMrr() {
			return mrr;
		}
		public String getArr() {
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
		public boolean isChurn() {
			return churn;
		}
		public String getSupportLevel() {
			return supportLevel;
		}

		@Override
		public String toString() {
			return GSON.toJson(this);
		}
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
		numEmp,
		annRev,
		mrr,
		stage,
		gORl,
		industry,
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