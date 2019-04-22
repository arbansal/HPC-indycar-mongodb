package iu.edu.indycar.mongo;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;


public class TrackService {

	private MongoDatabase mongoDatabase;
    private MongoCollection<Document> tracks;

    public TrackService(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
        this.tracks = mongoDatabase.getCollection("trackinfo");
    }
	
	//List all the tracks
	public void getAll() {
		
		MongoCursor<String> files = tracks.distinct("track_name", String.class).iterator();
	    
		while(files.hasNext()) {
	      System.out.println(files.next());
	    }
	}

	//Get all the information of a track, when track name or id is given. 
	public MongoCursor<Document> getTrack(int trackId) {
		
		//currently set for track_name "Indianapolis Motor Speedway"
		MongoCursor<Document> files = tracks.find(Filters.eq("track_name", "Indianapolis Motor Speedway")).projection(Projections.exclude("_id")).iterator();
		
		while(files.hasNext()) {
		      System.out.println(files.next());
		    }
		return files;
	}	
}
