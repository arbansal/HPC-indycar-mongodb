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
	public MongoCursor<String> getAll() {
		
		MongoCursor<String> files = drivers.distinct("driver_name", String.class).iterator();
	    while(files.hasNext()) {
	      System.out.println(files.next());
	    }
	    return files;
	}
	
	//Search driver by name. (Typing Ca, should return Ed Carpenter, Carl, etc)
	public MongoCursor<String> search(String keyword){
		
		//currently hardcoded for keyword "al", case insensitive
		MongoCursor<String> files = drivers.distinct("driver_name",Filters.regex("driver_name","al","i"),String.class).iterator();
		while(files.hasNext()) {
	        System.out.println(files.next());
	      }
		return files;
	}
	
	
	//Get Set of drivers for race
	public MongoCursor<String> getDriversByRace(String race){
		
		//currently set for race_name "Indycar"
		MongoCursor<String> files = drivers.distinct("driver_name",Filters.eq("race_name","IndyCar"), String.class).iterator();
		while(files.hasNext()) {
	        System.out.println(files.next());
	      }
		return files;
	}
	
	//Get the driver’s profile 
	public Document getDriver(int driverId){
		
		//currently set for driver_id "385"
	    Document files = drivers.find(Filters.eq("driver_id", "658")).projection(Projections.include("driver_name","driver_team","driver_hometown")).first();
	    System.out.println(files);
	    return files;
	}
	
	//Get Lap records and lap section records when driver and race is given.
	public MongoCursor<String> getLapRecords(String race_name, int driverId){

		//currently set for driver_id "385"
		MongoCursor<String> files = drivers.distinct("driver_name",Filters.eq("driver_id","385"), String.class).iterator();
		while(files.hasNext()) {
	        System.out.println(files.next());
	      }
		return files;
	}
	
}
