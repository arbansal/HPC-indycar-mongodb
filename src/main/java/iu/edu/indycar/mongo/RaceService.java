package iu.edu.indycar.mongo;

import java.util.List;

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
import java.util.Date;
import java.util.ArrayList;
import java.util.function.Consumer;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;

import java.sql.DatabaseMetaData;


public class RaceService {
	
	private MongoDatabase mongoDatabase;
	private MongoCollection<Document> drivers;
    private MongoCollection<Document> races;
    private MongoCollection<Document> overallresults;
    private MongoCollection<Document> telemetry;

    public RaceService(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
        this.drivers = mongoDatabase.getCollection("driverinfo");
        this.races = mongoDatabase.getCollection("runinfo");
        this.overallresults = mongoDatabase.getCollection("overallresults");
        this.telemetry = mongoDatabase.getCollection("telemetry");
    }
	
	//List all races(just the name and id)
	public List<String> getAll() {
		
		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = drivers.distinct("race_name", String.class).iterator();
	    while(files.hasNext()) {
	    	docList.add(files.next());
	    }
	    return docList;
	}
	
	// Distinct Drivers given race name | Get Set of drivers for race
	public List<String> getdriverbyRace(String racename){
		
		List<String> docList = new ArrayList<String>();
		//currently set for race_name "IndyCar"
		MongoCursor<String> files = drivers.distinct("driver_name",Filters.eq("race_name",racename), String.class).iterator();
		while(files.hasNext()) {
	    	docList.add(files.next());
	    }
	    return docList;
	}	
	
	//Get the ranks of the race
	public List<Document> getRanks(String racename){		
		
		List<Document> docList = new ArrayList<Document>();
		MongoCursor<Document> files = overallresults.find(Filters.eq("Class", racename)).
	               projection(Projections.include("Overall Rank",
	               "Team","Driver ID","date")).iterator();
		
		while(files.hasNext()) {
	    	docList.add(files.next());
	    }
	    return docList;
	}
	
	//Get a snapshot of a race at given time. (Including position,speed,rpm, anomaly scores etc of each car at the given time. 
	//Also include other race related features such as flags).
	public List<Document> snapshot(String timestamp) {
		
		List<Document> docList = new ArrayList<Document>();
		MongoCursor<Document> files = telemetry.find(Filters.regex("time_of_day",timestamp)).iterator();
		while(files.hasNext()) {
	    	docList.add(files.next());
	    }
	    return docList;
	}
	
	//Get all the flags of a given race.
	public List<String> getflagsbyRace(String racename){
		
		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = overallresults.distinct("Flag Status",Filters.eq("Class",racename), String.class).iterator();
		while(files.hasNext()) {
	    	docList.add(files.next());
	    }
	    return docList;
	}
	
}
