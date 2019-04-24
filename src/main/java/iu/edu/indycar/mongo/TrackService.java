package iu.edu.indycar.mongo;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoCollection;

import java.util.ArrayList;
import java.util.List;

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
	public List<String> getAll() {
		
		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = tracks.distinct("track_name", String.class).iterator();
		while(files.hasNext()) {
	    	docList.add(files.next());
	    }
	    return docList;
	}

	//Get all the information of a track, when track name or id is given. 
	public List<Document> getTrack(String trackname) {
		
		List<Document> docList = new ArrayList<Document>();
		MongoCursor<Document> files = tracks.find(Filters.eq("track_name", "Indianapolis Motor Speedway")).projection(Projections.exclude("_id")).iterator();
		
		while(files.hasNext()) {
	    	docList.add(files.next());
	    }
	    return docList;
	}	
}
