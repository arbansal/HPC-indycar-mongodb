package iu.edu.indycar.mongo;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

public class TrackService {

	private MongoDatabase mongoDatabase;
	private MongoCollection<Document> tracks;

	public TrackService(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
		this.tracks = mongoDatabase.getCollection("trackinfo");
	}

	// List all the tracks
	public List<String> getAll() {

		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = tracks.distinct("track_name", String.class).iterator();
		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}

	// Get all the information of a track, when track name or id is given.
	public List<Document> getTrack(String trackname) {

		List<Document> docList = new ArrayList<Document>();
		MongoCursor<Document> files = tracks.find(Filters.eq("track_name", trackname))
				.projection(Projections.exclude("_id")).iterator();

		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}
}