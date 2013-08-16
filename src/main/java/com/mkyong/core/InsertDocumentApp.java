package com.mkyong.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
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
	private static Map<String, String> AcctMappping = new HashMap<String, String>();
	private static Map<String, Integer> AcctScoring = new HashMap<String, Integer>();
	private static Map<String, String> AcctNameMapping = new HashMap<String, String>();
	private static final String ipAddr = "localhost";
	
	@Test
	public void insert() throws Exception {

		appendHealthScore("/data/mydata/mahout/dataset/hostAnalytics/productData/endUserNEventCounts2.tsv");
		insertBNAaccountObject("/data/mydata/mahout/dataset/hostAnalytics/productData/bnaAcctsData.tsv");
//		insertBNAenduserObject("/data/mydata/mahout/dataset/hostAnalytics/productData/acctnamesNuser.tsv");
		
//		for (Entry<String, String> entry : AcctMappping.entrySet())
//			System.out.printf("%s\t%s\n", entry.getKey(), entry.getValue());;
	}

	@Test
	public void retrieve() {

		try {
			Mongo mongo = new Mongo(ipAddr, 27017);
			DB db = mongo.getDB("bow");

			DBCollection user = db.getCollection("account");
			
			BasicDBObject whereQuery = new BasicDBObject();
//			whereQuery.put("name", "vamsi krishna"); 
//			whereQuery.put("accountId", new ObjectId("52051655e4b0b995b2e11c62")); 
			DBCursor cursor = user.find(whereQuery);
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}
			System.out.println(user.count());
			cursor.close();
			
			DBCollection account = db.getCollection("account");
			whereQuery = new BasicDBObject();
//			whereQuery.put("name", "mirion technologies inc");
			whereQuery.put("name", "mirion technologies inc");
//			whereQuery.put("_id", new ObjectId("52051655e4b0b995b2e11c62"));
			
			DBCursor cursorDoc = account.find(whereQuery);
			while (cursorDoc.hasNext()) {
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

	void insertBNAaccountObject(String srcFile) throws Exception {

		try {
			Mongo mongo = new Mongo(ipAddr, 27017);
			DB db = mongo.getDB("bow");

			DBCollection table = db.getCollection("account");
			table.drop();
			table = db.getCollection("account");
			
			List<DBObject> feeds = new LinkedList<DBObject>();
			int threshold = 1000;
			
			File sFile = new File(srcFile);
			BufferedReader br = new BufferedReader(new FileReader(sFile));
			
			String line = null;
			line = br.readLine();

		    int redCnt = 0;
		    int greenCnt = 0;
		    int yellowCnt = 0;
		    
			while (threshold-- > 0 && (line = br.readLine()) != null) {

				BasicDBObject mydbObject = new BasicDBObject();
				BNAacct acct = new BNAacct(line);
				mydbObject.put("_id", acct.get_id());
				mydbObject.put("annualSpend", acct.getAnnualSpend());
				mydbObject.put("csmName", acct.getCsmName());
				mydbObject.put("salesLead", acct.getSalesLead());
				mydbObject.put("endUserCount", acct.getEndUserCount());
				mydbObject.put("location", acct.getLocation());
				mydbObject.put("name", acct.getName());
				mydbObject.put("region", acct.getRegion());
				mydbObject.put("stage", acct.getStage());
				mydbObject.put("tier", acct.getTier());
				String uAcctName = getSimilarAcctFromUsage(acct.getName());
				Integer score = AcctScoring.get(uAcctName);
				if (score == null) {
					System.out.printf("no mapping acct for %s\n", acct.getName());
					score = 0;
				}	
				
		    	Double fVal = 100.0 * Math.exp((1.0 * score)/3792.0)/Math.exp(1);
		    	String scoreCard = "atRisk";
		    	if (fVal.compareTo(41.5) > 0) {
		    		scoreCard = "stable";
		    		greenCnt++;
		    	}
		    	else if (fVal.compareTo(37.0) > 0) {
		    		scoreCard = "nearAtRisk";
		    		yellowCnt++;	
		    	}
		    	else 
		    		redCnt++;
		    	
				mydbObject.put("healthScore", scoreCard);
				
				AcctMappping.put(acct.getName(), acct.get_id().toString());
								
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
			
			float total = redCnt + yellowCnt + greenCnt;
		    System.out.printf("RED=%f\tYELLOW=%f\tGREEN=%f\n", 100.0*redCnt/total, 100.0*yellowCnt/total, 100.0*greenCnt/total);
		    
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		double d3 = Math.exp(1.0/2.0);
		System.out.println(String.valueOf(d3));
	}
	
	
	public static void main2(String[] args) {

		try {

			Mongo mongo = new Mongo("localhost", 27017);
			DB db = mongo.getDB("bow");

			DBCollection collection = db.getCollection("accts");
			collection.drop();
			collection = db.getCollection("accts");

			// 1. BasicDBObject example
			System.out.println("BasicDBObject example...");
			BasicDBObject document = new BasicDBObject();
			document.put("_id", _idGenerator.get());
			document.put("database", "mkyongDB");
			document.put("table", "hosting");

			BasicDBObject documentDetail = new BasicDBObject();
			documentDetail.put("_id", _idGenerator.get());
			documentDetail.put("records", 99);
			documentDetail.put("index", "vps_index1");
			documentDetail.put("active", "true");
			document.put("detail", documentDetail);

			collection.insert(document);

			DBCursor cursorDoc = collection.find();
			while (cursorDoc.hasNext()) {
				System.out.println(cursorDoc.next());
			}

			collection.remove(new BasicDBObject());

			// 2. BasicDBObjectBuilder example
			System.out.println("BasicDBObjectBuilder example...");
//			BasicDBObjectBuilder documentBuilder = BasicDBObjectBuilder.start()
//					.add("database", "mkyongDB").add("table", "hosting");
//
//			BasicDBObjectBuilder documentBuilderDetail = BasicDBObjectBuilder
//					.start().add("records", "99").add("index", "vps_index1")
//					.add("active", "true");
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
//			documentMap.put("database", "mkyongDB");
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
//			// 4. JSON parse example
//			System.out.println("JSON parse example...");
//
//			String json = "{'database' : 'mkyongDB','table' : 'hosting',"
//					+ "'detail' : {'records' : 99, 'index' : 'vps_index1', 'active' : 'true'}}}";
//
//			DBObject dbObject = (DBObject) JSON.parse(json);
//
//			collection.insert(dbObject);
//
//			DBCursor cursorDocJSON = collection.find();
//			while (cursorDocJSON.hasNext()) {
//				System.out.println(cursorDocJSON.next());
//			}
//
//			collection.remove(new BasicDBObject());
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	static String getSimilarAcctFromAcct(final String uAcctName) {
		String matchedName = uAcctName;
		String uAcctNameNew = uAcctName.replaceAll(" ", "");
		
		int count = 0;
		int maxMatches = 0;
		String[] words = null;
		for (String aAcctName : AcctMappping.keySet()) {
			count = 0;
			
			if (aAcctName.contains("aaxico"))
				count = 0;
			
			words = adjustedName(aAcctName).split(" ");
			for (String word : words) {
				if (word == null || word.isEmpty())
					continue;
				
				if (uAcctNameNew.contains(word))
					count++;
			}
			
			if (count > maxMatches)
				matchedName = aAcctName;
		}
		
		return matchedName;
	}


	static String getSimilarAcctFromUsage(final String acctName) {
		String matchedName = acctName;
		String acctNameNew = acctName.replaceAll(" ", "");
		
		int count = 0;
		int maxMatches = 0;
		String[] words = null;
		for (String uAcctName : AcctScoring.keySet()) {
			count = 0;
			
			words = adjustedName(uAcctName).split(" ");
			for (String word : words) {
				if (word == null || word.isEmpty())
					continue;
				
				if (acctNameNew.contains(word))
					count++;
			}
			
			if (count > maxMatches)
				matchedName = uAcctName;
		}
		
		AcctNameMapping.put(matchedName, acctName);
		
		return matchedName;
	}
	
	void appendHealthScore(String srcFile) throws IOException {

		File sFile = new File(srcFile);
		BufferedReader br = new BufferedReader(new FileReader(sFile));
		
		String line = br.readLine();
		String[] splits = null; 
		
		while ((line = br.readLine()) != null) {
			splits = line.split("\t");
			String acctName = splits[1].trim();
			// TODO:: change into String for float into integer
			Integer score = Integer.parseInt(splits[0]);
			
			AcctScoring.put(acctName, score);
		}
		
		br.close();
	}

	void insertBNAenduserObject(String srcFile) throws Exception {

		try {
			Mongo mongo = new Mongo(ipAddr, 27017);
			DB db = mongo.getDB("bow");

			DBCollection table = db.getCollection("endUser");
			table.drop();
			table = db.getCollection("endUser");
			
			List<DBObject> feeds = new LinkedList<DBObject>();
			int threshold = 1000;
			
			File sFile = new File(srcFile);
			BufferedReader br = new BufferedReader(new FileReader(sFile));
			
			String line = null;
			line = br.readLine();
			int total = 0;
			int invalid = 0;
			
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
				mydbObject.put("name", ensuser.getName());				
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
	

	public static class BNAendUser {
		private Object _id = null;
		private Object accountId = null;
		private String name = null;
		
		public BNAendUser(String data) {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			if (splits.length != 3)
				return;
			
			String uAcctName = splits[0].trim();
			this.name = splits[2];
			
			this._id = new ObjectId();

			String acctId = AcctMappping.get(getSimilarAcctFromAcct(uAcctName));
			if (uAcctName.contains("mirion"))
				System.out.printf("mirion:: uAcctName=%s\taAcctName=%s\n", 
						uAcctName, 
						getSimilarAcctFromAcct(uAcctName));
			
			if (acctId == null) {
				this.accountId = null;
				System.out.printf("invalid uAcctName=%s\taAcctName=%s\n", 
						uAcctName, 
						getSimilarAcctFromAcct(uAcctName));
			}
			else
				this.accountId = new ObjectId(acctId);
		}

		public Object get_id() {
			return _id;
		}
		public Object getAccountId() {
			return accountId;
		}
		public String getName() {
			return name;
		}

	}
	
	static Set<String> StopWords = new HashSet<String>();
	
	static {
		StopWords.add("inc");
		StopWords.add("group");
		StopWords.add("limited");
		StopWords.add("ltd");
		StopWords.add("global");
		StopWords.add("technologies");
		StopWords.add("company");
		StopWords.add("co");
		StopWords.add("corp");
		StopWords.add("the");
		StopWords.add("new");
		StopWords.add("old");
		StopWords.add("a");
		StopWords.add("and");
		StopWords.add("llc");
		StopWords.add("l.l.c");
		StopWords.add("international");
		StopWords.add("incorporated");
		StopWords.add("corporation");
	}
	
	static String adjustedName(String name) {
		StringBuilder buf = new StringBuilder();
		
		List<String> words = new ArrayList<String>();
		
		if (name != null && name.isEmpty() == false) {
			String[] splits = name.split(" ");
			
			for (String word : splits) {
				if (word.isEmpty())
					continue;
				
				if (StopWords.contains(word) == false)
					words.add(word);
			}
		}
		
		for (String word : words)
			buf.append(word).append(" ");
		
		return buf.toString().trim();
	}
	
	public static class BNAacct {
		private Object _id = null;
		private String name = null;
		private String csmName = null;
		private String salesLead = null;
		private String location = null;
		private String region = null;
		private String endUserCount = null;
		private String annualSpend = null;
		private String tier = null;
		private String stage = null;
		
		public BNAacct(String data) {
			if (data == null || data.isEmpty())
				return;
			
			String[] splits = data.split("\t");
			if (splits.length != 9)
				return;
			
			this.name = splits[0].trim();
			this.csmName = splits[1];
			this.salesLead = splits[2];
			this.location = splits[3];
			this.region = splits[4];
			this.endUserCount = splits[5];
			this.annualSpend = splits[6];
			this.tier = splits[7];
			this.stage = splits[8];
			
			this._id = new ObjectId();
		}
		
		public Object get_id() {
			return _id;
		}
		public String getName() {
			return name;
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
		public String getAnnualSpend() {
			return annualSpend;
		}
		public String getTier() {
			return tier;
		}
		public String getStage() {
			return stage;
		}

		@Override
		public String toString() {
			return new Gson().toJson(this);
		}
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
			return new Gson().toJson(this);
		}
	}
	
//	String toJson(String line) throws Exception {
//		
//		String retCode = "";
//		if (StringUtil.isNullOrEmpty(line))
//			return retCode;
//		
//		String[] splits = line.split(CommonTokens.UNICODE_FILED_SEP);
//		if (splits.length < BNAticketFields.description.ordinal())
//			return retCode;
//		
//		StringBuilder buf = new StringBuilder("{\"id\":\"");
//		buf.append(splits[0]).append("\"");
//		for (BNAticketFields attr : BNAticketFields.values()) {
//			if (attr.ordinal() == 0 || BNAticketFields.description.equals(attr))
//				continue;
//			else
//				buf.append(", ");
//				
//			if (BNAticketFields.closed.equals(attr) || BNAticketFields.resolved.equals(attr) || BNAticketFields.deleted.equals(attr))
//				buf.append("\"").append(attr.name()).append("\":").append(splits[attr.ordinal()]);
//			else if (BNAticketFields.reason.equals(attr)) {
//				buf.append("\"").append(attr.name()).append("\":\"").append(splits[attr.ordinal()].replaceAll("\"", "'")).append("\"");
//			} else if (BNAticketFields.contactId.equals(attr)) {
//				buf.append("\"").append(attr.name()).append("\":").append(splits[attr.ordinal()]).append("");
//			} else if (BNAticketFields.createdDate.equals(attr)) {
//				String dateStr = splits[attr.ordinal()];
//				DateWrapper dp = new DateWrapper(dateStr, "yyyy-MM-dd");
//				buf.append("\"").append(attr.name()).append("\":").append(dp.getCurrentDateStr().replaceAll("-", "")).append("");
//			} else if (BNAticketFields.type.equals(attr)) {
//				buf.append("\"").append(attr.name()).append("\":").append(splits[attr.ordinal()]).append("");
//			}
//			else
//				buf.append("\"").append(attr.name()).append("\":\"").append(splits[attr.ordinal()]).append("\"");
//		}
//		
//		buf.append("}");
//		
//		retCode = buf.toString();
//		
//		return retCode;
//	}
}