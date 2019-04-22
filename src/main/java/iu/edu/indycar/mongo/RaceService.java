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
    private MongoCollection<Document> races;
    private MongoCollection<Document> overallresults;
    private MongoCollection<Document> telemetry;

    public RaceService(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
        this.races = mongoDatabase.getCollection("runinfo");
        this.overallresults = mongoDatabase.getCollection("overallresults");
        this.telemetry = mongoDatabase.getCollection("telemetry");
    }
	
	//List all races(just the name and id)
	public void getAll() {
		
		MongoCursor<String> files = races.distinct("race_name", String.class).iterator();
	    while(files.hasNext()) {
	      System.out.println(files.next());
	    }
	}
	
		//Get all race metadata
	public MongoCursor<String> getRace(String racename){
		
		//currently set for race_name "IndyCar"
		MongoCursor<String> files = races.distinct("race_name",Filters.eq("race_name","385"), String.class).iterator();
		while(files.hasNext()) {
	        System.out.println(files.next());	
		}
		return files;
	}	
	
	//Get the ranks of the race
	public MongoCursor<Document> getRanks(String racename){		
//		MongoCollection<Document> collection = database.getCollection("overallresults");
		
		MongoCursor<Document> files = overallresults.find(Filters.eq("Class", "IndyCar")).
	               projection(Projections.include("Overall Rank",
	               "Team","Driver ID","date")).iterator();
		
		while(files.hasNext()) {
	        System.out.println(files.next());
	      }	
		return files;
	}
	
	//Get a snapshot of a race at given time. (Including position,speed,rpm, anomaly scores etc of each car at the given time. 
	//Also include other race related features such as flags).
	public MongoCursor<Document> snapshot(Date timestamp) {
	
		MongoCursor<Document> files = (mongoDatabase.getCollection("telemetry")).find(Filters.eq("time_of_day","9:29:08.322")).iterator();
		while(files.hasNext()) {
	        System.out.println(files.next());
	      }
		return files;
	}
	
	//Get all the flags of a given race.
	public MongoCursor<String> getFlags(String racename){
		
//		MongoCollection<Document> collection = database.getCollection("overallresults");
		//currently set to Class 'IndyCar'
		MongoCursor<String> files = overallresults.distinct("Flag Status",Filters.eq("Class","IndyCar"), String.class).iterator();
		while(files.hasNext()) {
	        System.out.println(files.next());
	      }
		return files;
	}
	
}
