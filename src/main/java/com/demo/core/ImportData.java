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
public class ImportData {
	
	private static final ObjectId _idGenerator = new ObjectId();
	private static Map<String, String> AcctMappping = null;
	private static Map<String, Double> AcctScoring = null;
	private static Map<String, AcctChurnNotherDates> ChurnRenewalDates = null;
	private static final String ipAddr = "10.0.9.18"; // "ec2-23-22-156-75.compute-1.amazonaws.com";
	private static final String dbName = "bow-replicon"; // "bow-fa8e12345678900000000001";
	private Map<String, LinkedHashSet<Health>> acctsHScores = null;
	private Map<String, LinkedHashSet<Health>> usersHScores = null;
	private static SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMdd");
	private static SimpleDateFormat usageDateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSS");
	private static SimpleDateFormat churnDateFormat = new SimpleDateFormat("MM/dd/yy");
	private static DecimalFormat decimalFormat = new DecimalFormat("##########.##");
	private static final Gson GSON = new Gson();

	@Test
	public void insert() {

		try {
			insertAcctHealthScore("/Users/borongzhou/test/replicon/generalAcctScores.tsv");
			insertBNAaccountObject("/Users/borongzhou/test/replicon/generalAcctMRRs.tsv");
			acctsHScores.clear();
			acctsHScores = null;

			insertUserHealthScore("/Users/borongzhou/test/replicon/generalEndUserScores.tsv");
			insertBNAenduserObject("/Users/borongzhou/test/replicon/generalEndUsers.tsv");

			usersHScores.clear();
			usersHScores = null;
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	@Before
	public void setup() {
		AcctMappping = new HashMap<String, String>();
		AcctScoring = new HashMap<String, Double>();
		acctsHScores = new HashMap<String, LinkedHashSet<Health>>();
		usersHScores = new HashMap<String, LinkedHashSet<Health>>();
		ChurnRenewalDates = new HashMap<String, AcctChurnNotherDates>();
	}
	
	@After
	public void teranDown() {
		
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
			acctId = splits[2].trim();
			
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

			if (AcctMappping.get(acctId) == null) {
				invalidRecords++;
				continue;
			}
			
			userId = acctId + "-" + userId;
			
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
			
			int count = 20;
			while (count-- > 0 && cursor.hasNext()) {
				System.out.println(cursor.next());
			}
			cursor.close();
			
			DBCollection account = db.getCollection("account");
			whereQuery = new BasicDBObject();
//			whereQuery.put("name", "mirion technologies inc");
//			whereQuery.put("_id", new ObjectId("52051655e4b0b995b2e11c62"));
			
			DBCursor cursorDoc = account.find(whereQuery);
			count = 20;
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
			int totalRecords = 0;
			
			String line = null;		
			while (threshold-- > 0 && (line = br.readLine()) != null) {

				if (line.contains("acctId"))
					continue;
				totalRecords++;
				
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
				mydbObject.put("name", acct.getAcctName());
				mydbObject.put("region", acct.getRegion());
				mydbObject.put("stage", acct.getStage()); // default
				mydbObject.put("tier", acct.getTier());	
				mydbObject.put("industry", acct.getIndustry());
				mydbObject.put("campaign", acct.getCampaign());
				mydbObject.put("channel", acct.getChannel());
				mydbObject.put("contractDate", acct.getContractDate());
				mydbObject.put("renewalDate", acct.getRenewalDate()); // default
				mydbObject.put("churnDate", acct.getChurnDate());	
				mydbObject.put("firstEvent", acct.getFirstEvent());
				mydbObject.put("lastEvent", acct.getLastEvent());				
				mydbObject.put("healthScores", acctsHScores.get(acct.getAcctId()));

				AcctMappping.put(acct.getAcctId(), acct.get_id().toString());
								
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

			System.out.printf("Total insert Accts %d\n", totalRecords);
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args)  {
		
		try {
			Mongo mongo = new Mongo(ipAddr, 27017);
			DB db = mongo.getDB(dbName);
			DBCollection collection = db.getCollection("account");
			collection.drop();
			collection = db.getCollection("endUser");
			collection.drop();
			
			ImportData demo = new ImportData();
			
			demo.setup();
			demo.insert();
			demo.teranDown();
			
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

		} catch(Throwable ex) {
			ex.printStackTrace(System.out);
		}
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
			while (threshold-- > 0 && (line = br.readLine()) != null) {

				if (line.contains("acctId"))
					continue;
				
				BasicDBObject mydbObject = new BasicDBObject();
				BNAendUser enduser = new BNAendUser(line);

				total++;
				if (enduser.getAccountId() == null) {
					invalid++;
					continue;
				}
				
				mydbObject.put("_id", enduser.get_id());
				mydbObject.put("accountId", enduser.getAccountId());
				mydbObject.put("name", enduser.getName());	
				mydbObject.put("firstEvent", enduser.getFirstEvent());
				mydbObject.put("lastEvent", enduser.getLastEvent());
				String userId = enduser.getAcctId() + "-" + enduser.getUserId();
				LinkedHashSet<Health> userHScore = usersHScores.get(userId);
				if (userHScore == null) {
					System.out.printf("no heath score for user %s\t acct %s\n", enduser.getUserId(), enduser.getAccountId());
				}
				mydbObject.put("healthScores", userHScore);
				
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
	
	public static class AcctChurnNotherDates {
		
		Date startDate = null;
		Date renewalDate = null;
		Date churnDate = null;
		
		public AcctChurnNotherDates(String startStr, String renewalStr, String churnStr) {
			
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
	
	public static class BNAendUser {
		private Object _id = null;
		private Object accountId = null;
		private String acctId = null;
		private String userId = null;
		private String name = null;
		private Date firstEvent = null;
		private Date lastEvent = null;
		
		public BNAendUser(String data) throws ParseException {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			if (splits.length != 5)
				return;
			
			this.userId = splits[1];
			this.name = splits[2];
			String str = splits[3];
			this.firstEvent = "-9999".equals(str)? null : usageDateFormat.parse(str);
			str = splits[4];
			this.lastEvent = "-9999".equals(str)? null : usageDateFormat.parse(str);
			this.acctId = splits[0].trim();
			if (AcctMappping.get(acctId) == null) {
				System.out.println("no objectId for " + acctId);
			}
			else
				this.accountId = new ObjectId(AcctMappping.get(acctId));

			this._id = new ObjectId();
		}

		public Object get_id() {
			return _id;
		}
		public Object getAccountId() {
			return accountId;
		}
		public String getUserId() {
			return userId;
		}
		public String getName() {
			return name;
		}
		public Date getFirstEvent() {
			return firstEvent;
		}
		public Date getLastEvent() {
			return lastEvent;
		}
		public String getAcctId() {
			return acctId;
		}

		@Override
		public String toString() {
			return GSON.toJson(this);
		}
	}
	
	public static class BNAacct {
		private Object _id = null;
		private String acctId = null;
		private String acctName = null;
		private String csmName = null;
		private String salesLead = null;
		private String location = null;
		private String region = null;
		private String endUserCount = null;
		private String mrr = null;
		private String arr = null;
		private String industry = null;
		private String tier = null;
		private String stage = null;
		private String campaign = null;
		private String channel = null;
		private Date contractDate = null;
		private Date renewalDate = null;
		private Date churnDate = null;
		private Date firstEvent = null;
		private Date lastEvent = null;
		
		public BNAacct(String data) throws ParseException {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			if (splits.length != 17)
				return;
			
			this.acctId = splits[AcctFields.acctId.ordinal()].trim();
			this.acctName = splits[AcctFields.acctName.ordinal()].trim();
			this.csmName = splits[AcctFields.csmName.ordinal()].trim();
			this.salesLead = splits[AcctFields.salesLead.ordinal()];
			this.location = splits[AcctFields.location.ordinal()];
			this.region = splits[AcctFields.region.ordinal()];
			this.endUserCount = splits[AcctFields.endUserCount.ordinal()];
			this.mrr = splits[AcctFields.mrr.ordinal()]; // decimalFormat
			double mrrDouble = Double.parseDouble(mrr);
			this.mrr = decimalFormat.format(mrrDouble);
			this.arr = decimalFormat.format(12 * mrrDouble);
			this.industry = splits[AcctFields.industry.ordinal()];
			this.tier = splits[AcctFields.tier.ordinal()];
			this.campaign = splits[AcctFields.campaign.ordinal()];
			this.channel = splits[AcctFields.channel.ordinal()];
			String str = splits[AcctFields.contracteDate.ordinal()];
			this.contractDate = "-9999".equals(str)? null : churnDateFormat.parse(str);
			str = splits[AcctFields.churnDate.ordinal()]; 
			this.churnDate = "-9999".equals(str)? null : churnDateFormat.parse(str);
			str = splits[AcctFields.firstEventDate.ordinal()];
			this.firstEvent = "-9999".equals(str)? null : usageDateFormat.parse(str);
			str = splits[AcctFields.lastEventDate.ordinal()];
			this.lastEvent = "-9999".equals(str)? null : usageDateFormat.parse(str);
			
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
		public String getIndustry() {
			return industry;
		}
		public String getTier() {
			return tier;
		}
		public String getStage() {
			return stage;
		}
		public String getCampaign() {
			return campaign;
		}
		public String getChannel() {
			return channel;
		}
		public Date getContractDate() {
			return contractDate;
		}
		public Date getRenewalDate() {
			return renewalDate;
		}
		public Date getChurnDate() {
			return churnDate;
		}
		public Date getFirstEvent() {
			return firstEvent;
		}
		public Date getLastEvent() {
			return lastEvent;
		}

		@Override
		public String toString() {
			return GSON.toJson(this);
		}
	}
	
	public static enum AcctFields {
		acctId,
		acctName,
		csmName,
		salesLead,
		location,
		region,
		endUserCount,
		mrr,
		arr,
		industry,
		tier,
		campaign,
		channel,
		contracteDate,
		churnDate,
		firstEventDate,
		lastEventDate
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