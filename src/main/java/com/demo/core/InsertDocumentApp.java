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
import com.mongodb.BasicDBObjectBuilder;
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

	private static final String ipAddr = "ec2-54-214-129-200.us-west-2.compute.amazonaws.com"; //"10.0.9.2"; //"ec2-23-22-156-75.compute-1.amazonaws.com";
	private static final int port = 37017; //  37017; // 27017; //  
	private static final String dbName = "bow-bna"; // "bow-fa8e12345678900000000000"; // "bow-openvpn"; // "bow-bna";
	private static final String username = "bnaadmin";
	private static final String password = "bluenose!";
			
	private Map<String, LinkedHashSet<Health>> acctsHScores = null;
	private Map<String, LinkedHashSet<Opportunity>> acctOppties = null;
	private Map<String, LinkedHashSet<Health>> usersHScores = null;
	private static SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMdd");
	private static DecimalFormat dFormat = new DecimalFormat("################.##");
	private static SimpleDateFormat usageDateFormat = new SimpleDateFormat("yyyyMMdd"); //"yyyy-MM-dd hh:mm:ss.SSS");
	private static final Gson GSON = new Gson();

//	@Test
	public void insertOpenVPN()  {
		
		try {
			insertOpenAcctHealthScore("/Users/borongzhou/test/openVPN/acctHealthScores.tsv");
			insertOpenAccountObject("/Users/borongzhou/test/openVPN/acctsFromAcct.tsv");
			
		} catch(Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	
	@Test
	public void insertHost()  {
		
		try {
			loadEvents();
			insertAcctHealthScore("/Users/borongzhou/test/fake/product/acctHealthScoreExt.tsv");
//			insertOpportunityObject("/Users/borongzhou/test/bnaAnalytics/product2/hostOppty.tsv");
			insertBNAaccountObject("/Users/borongzhou/test/fake/product/acctsFromAcct.tsv", "524c9ffbf7864895bdd8ee69");
			acctsHScores.clear();
			acctsHScores = null;
//			acctOppties.clear();
//			acctOppties = null;
		
			insertUserHealthScore("/Users/borongzhou/test/fake/product/endUserHealthScoreExt.tsv");
			insertBNAenduserObject("/Users/borongzhou/test/fake/product/endUserFromUsage.tsv");
			
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
		acctOppties = new HashMap<String, LinkedHashSet<Opportunity>>();
		ChurnRenewalDates = new HashMap<String, AcctDates>();
	}
	
	@After
	public void tearDown() {

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

		if (acctOppties != null) {
			acctOppties.clear();
			acctOppties = null;
		}
		
		ChurnRenewalDates.clear();
		ChurnRenewalDates = null;
		
		sFormat = null;
		usageDateFormat = null;
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
			acctId = splits[1].toLowerCase().trim();
			
			list = acctsHScores.get(acctId);
			if (list == null) {
				list = new LinkedHashSet<Health>();
				acctsHScores.put(acctId, list);
			}
			score = new Health();
			int myScore = Integer.parseInt(splits[0]);
			score.setScore((int)myScore);
			score.setCreated(sFormat.parse(splits[3] + "01"));
			
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


	@Test
	public void retrieve() {

		try {
//			Mongo mongo = new Mongo(ipAddr, port);
//			DB db = mongo.getDB(dbName);
			
			MongoClient mongoClient = new MongoClient(ipAddr, port);
			DB db = mongoClient.getDB("bow-bna");
			boolean auth = db.authenticate(username, password.toCharArray());
			if (!auth) {
				System.out.println("authentication error!");
				System.exit(1);	
			}
			
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
		
		public void set(String data) {

			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
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
			
			return this.opptyId;
//			return new Gson().toJson(this);
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
			oppty.set(line);

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
//			Mongo mongo = new Mongo(ipAddr, port);
//			DB db = mongo.getDB(dbName);

			MongoClient mongoClient = new MongoClient(ipAddr, port);
			DB db = mongoClient.getDB(dbName);
			boolean auth = db.authenticate(username, password.toCharArray());
			if (!auth) {
				System.out.println("authentication error!");
				System.exit(1);
			}

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
			while (threshold-- > 0 && (line = br.readLine()) != null) {

				BasicDBObject mydbObject = new BasicDBObject();
				BNAacct acct = new BNAacct(line);
				mydbObject.put("customerId", custId);
				mydbObject.put("_id", acct.get_id());
				mydbObject.put("usageId", acct.getAcctId());
				mydbObject.put("arr", acct.getArr());
//				mydbObject.put("mrr", acct.getMrr());
				mydbObject.put("csmName", acct.getCsmName());
				mydbObject.put("salesLead", acct.getSalesLead());
				mydbObject.put("endUserCount", acct.getEndUserCount());
				mydbObject.put("location", acct.getLocation());
				mydbObject.put("name", acct.getName());
				mydbObject.put("region", acct.getRegion());
				mydbObject.put("stage", acct.getStage());
				mydbObject.put("tier", acct.getTier());		
				mydbObject.put("supportLevel", acct.getSupportLevel());
//				mydbObject.put("industry", acct.getIndustry());		
				mydbObject.put("churn", acct.isChurn());	
				if (acct == null || acct.getAcctId() == null) {
					System.out.printf("no acct for data %s\n", line); 
					continue;
				}
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
				
				String acctId = acct.getAcctId().toLowerCase();
				LinkedHashSet<Health> hscores = acctsHScores.get(acctId);
				if (hscores == null) {
					System.out.printf("no hscore for account %s\n", acctId);
				}
				mydbObject.put("healthScores", hscores);
//				mydbObject.put("accountOpportunity", acctOppties.get(acctId));

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
		
//		ObjectId objId = new ObjectId("5229f0663004e751ecdf841c");
//		
//		System.out.println(objId);
//		
//		for (int i = 0; i < 20; i++)
//			System.out.println(new ObjectId());
		
//		InsertDocumentApp app = new InsertDocumentApp();
//		app.setup();
//		app.insertOpenVPN();
//		app.tearDown();
		
		try {
			// mongodb://admin:password@localhost:27017/db
//			MongoClient.connect(addr);
			
			MongoClient mongoClient = new MongoClient(ipAddr, port);
			DB db = mongoClient.getDB("bow-openvpn");
			boolean auth = db.authenticate(username, password.toCharArray());
			if (!auth) {
				System.out.println("authentication error!");
				System.exit(1);
			}
			
			DBCollection collection = db.getCollection("account");
			collection.drop();
			collection = db.getCollection("account");

			InsertDocumentApp app = new InsertDocumentApp();
			app.setup();
			app.insertOpenVPN();
			app.tearDown();
			
//			// 2. BasicDBObjectBuilder example
//			System.out.println("BasicDBObjectBuilder example...");
//			BasicDBObjectBuilder documentBuilder = BasicDBObjectBuilder.start()
//					.add("database", "bow").add("table", "account");
//
//			BasicDBObjectBuilder documentBuilderDetail = BasicDBObjectBuilder
//					.start().add("_id", new ObjectId("5229f0663004e751ecdf841c")).add("records", "99")
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

		} catch(Throwable ex) {
			ex.printStackTrace(System.out);
		}
	}
		
	static void loadEvents() throws IOException {

		String acctDates = "/Users/borongzhou/test/fake/product/acctFirstLastDate.tsv";
		String userDates = "/Users/borongzhou/test/fake/product/userFirstLastDate.tsv";
		String churnDates = "/Users/borongzhou/test/fake/product/acctDates.tsv";
		
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
			UserDates.put(splits[2] + "\t" + splits[3], event);
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
//			Mongo mongo = new Mongo(ipAddr, port);
//			DB db = mongo.getDB(dbName);

			MongoClient mongoClient = new MongoClient(ipAddr, port);
			DB db = mongoClient.getDB(dbName);
			boolean auth = db.authenticate(username, password.toCharArray());
			if (!auth) {
				System.out.println("authentication error!");
				System.exit(1);
			}
			
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
				mydbObject.put("firstEvent", usageDateFormat.parse(ensuser.getFirstDT()));
				mydbObject.put("lastEvent", usageDateFormat.parse(ensuser.getLastDT()));
				
//				event = UserDates.get(ensuser.getUserId());
//				if (event == null) {
//					System.out.printf("no event for user: Name=%s\n", ensuser.getUserId());
//					continue;
//					
//				}
//				mydbObject.put("firstEvent", usageDateFormat.parse(event.getFirstDate()));
//				mydbObject.put("lastEvent", usageDateFormat.parse(event.getLastDate()));
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
		
		public BNAendUser(String data) {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			if (splits.length != 2)
				return;
			
			this.userId = splits[1];
			this._id = new ObjectId();

			String acctId = splits[0].toLowerCase().trim();
			this.acctId = acctId;
			if (AcctMappping.get(acctId) == null) {
				System.out.println("no objectId for " + acctId);
			}
			else
				this.accountId = new ObjectId(AcctMappping.get(acctId));
			
			FirstLastEvent dates = UserDates.get(splits[0] + "\t" + splits[1]);
			
			this.firstDT = dates.firstDate;
			this.lastDT = dates.lastDate;
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
		private int endUserCount = 0;
		private long arr = 0L;
		private long mrr = 0L;
		private String contractedDT = null;
		private String renewalDT = null;
		private String stage = null;
		private String gORl = null;
		private String industry = null;
		private boolean churn = false;
		
		public BNAacct(String data) {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			if (splits.length != BNA_ACCT.values().length)
				return;
			
			this.acctId = splits[BNA_ACCT.acctId.ordinal()].trim();
			this.acctName = splits[BNA_ACCT.acctName.ordinal()].trim();
			this.csmName = splits[BNA_ACCT.csmName.ordinal()].trim();
			this.salesLead = splits[BNA_ACCT.salesLead.ordinal()];
			this.tier = splits[BNA_ACCT.tier.ordinal()];
			this.location = splits[BNA_ACCT.state.ordinal()];
			this.region = splits[BNA_ACCT.region.ordinal()];
			this.supportLevel = splits[BNA_ACCT.supportLevel.ordinal()];
			this.endUserCount = Integer.parseInt(splits[BNA_ACCT.numOfEmp.ordinal()]);
			double arrDouble = Double.parseDouble(splits[BNA_ACCT.arr.ordinal()]);
			this.arr = (long) arrDouble*100;
//			this.mrr = splits[HOST_ACCT_V3.mrr.ordinal()];
			this.stage = splits[BNA_ACCT.stage.ordinal()];
			this.contractedDT = splits[BNA_ACCT.contractedDT.ordinal()];
			this.renewalDT = splits[BNA_ACCT.renewalDT.ordinal()];
//			this.gORl = splits[BNA_ACCT.gORl.ordinal()];
//			this.industry = splits[BNA_ACCT.industry.ordinal()];
			this.churn = Boolean.parseBoolean(splits[BNA_ACCT.churn.ordinal()]);
			
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
		public int getEndUserCount() {
			return endUserCount;
		}
		public long getMrr() {
			return mrr;
		}
		public long getArr() {
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
		public String getContractedDT() {
			return contractedDT;
		}
		public String getRenewalDT() {
			return renewalDT;
		}

		@Override
		public String toString() {
			return GSON.toJson(this);
		}
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

	static enum BNA_ACCT {
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