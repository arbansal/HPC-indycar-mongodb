package iu.edu.indycar.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.ConnectionString;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import java.util.Arrays;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;

import java.sql.DatabaseMetaData;

public class DriversService {

	private MongoDatabase mongoDatabase;
	private MongoCollection<Document> drivers;
	private MongoCollection<Document> overallresults;
	private MongoCollection<Document> sectionresults;

	public DriversService(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
		this.drivers = mongoDatabase.getCollection("driverinfo");
		this.overallresults = mongoDatabase.getCollection("overallresults");
		this.sectionresults = mongoDatabase.getCollection("sectionresults");
	}

	//List all the drivers
	public List<Document> getAll() {
		List<Document> drivers = new ArrayList<>();
		// taking uids only
		this.drivers.find().projection(new Document("uid", 1)).forEach((Consumer<Document>) drivers::add);
		return drivers;
	}

	public List<String> getAll1() {

		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = drivers.distinct("driver_name", String.class).iterator();
		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}

	//Search driver by name (typing Ca, should return Ed Carpenter, Carl, etc.)
	public List<String> search(String keyword) {

		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = drivers
				.distinct("driver_name", Filters.regex("driver_name", keyword, "i"), String.class).iterator();
		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}

	// Get Set of drivers for race
	public List<String> getDriversByRace(String search_race_name) {

		// currently set for race_name "Indycar"
		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = drivers.distinct("driver_name", Filters.eq("race_class", search_race_name), String.class).iterator();
		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}
	
	//Search driver by UID 
	public Document getDriver(String uid) {
		return this.drivers.find(new Document("uid", uid)).first();
	}
	
	//Get the driver�s profile
	public Document getDriver1(String driverId) {
		Document files = drivers.find(Filters.eq("driver_id", driverId))
				.projection(Projections.include("driver_name", "driver_team", "driver_home_town")).first();
		return files;
	}

	// Get Lap records and lap section records when driver and race is given. (Query 1 + Query 2 below)
	public List<String> getLapRecords(String race_name, String driverId) {

		//Subuery 1: To retrieve distinct records from overallresults collection 
		List<Document> docList = new ArrayList<Document>();
    
	    Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
       };
       
		overallresults.aggregate(Arrays.asList(Aggregates.match(Filters.eq("Class", race_name)),
	               Aggregates.match(Filters.eq("Driver_ID", driverId)))).forEach(printBlock);
	       
//	       while(((DBCursor) printBlock).hasNext()) {
//		    	docList.add((Document) ((DBCursor) printBlock).next());
//		    }
	       
	   //Subquery 2: To retrieve car_num from driver table and then retrieve records from sectionresults collection for given car_num
       Document files = drivers.find(Filters.eq("driver_id", driverId)).projection(Projections.include("car_num")).first();
//       System.out.println(files.get("car_num"));
       
       String car_number = (String) files.get("car_num");
       
       List<String> docList1 = new ArrayList<String>();
       MongoCursor<String> files1 = sectionresults.distinct("driver_name", Filters.eq("car_num", car_number), String.class).iterator();
		
		while (files1.hasNext()) {
			docList1.add(files1.next());
		}
	    return docList1;    
	}
}