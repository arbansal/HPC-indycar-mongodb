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

	public DriversService(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
		this.drivers = mongoDatabase.getCollection("driverinfo");
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
		MongoCursor<String> files = drivers.distinct("driver_name", Filters.eq("race_name", search_race_name), String.class).iterator();
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
	public Document getDriver1(int driverId) {

		// currently set for driver_id "385"
		Document files = drivers.find(Filters.eq("driver_id", "658"))
				.projection(Projections.include("driver_name", "driver_team", "driver_hometown")).first();
		return files;
	}

	// Get Lap records and lap section records when driver and race is given.
	public MongoCursor<String> getLapRecords(String race_name, int driverId) {

		// currently set for driver_id "385"
		MongoCursor<String> files = drivers.distinct("driver_name", Filters.eq("driver_id", "385"), String.class)
				.iterator();
		while (files.hasNext()) {
			System.out.println(files.next());
		}
		return files;
	}

}