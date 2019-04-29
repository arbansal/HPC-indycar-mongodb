package iu.edu.indycar.mongo;

import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Arrays.asList;

import java.util.ArrayList;

import com.mongodb.client.MongoCursor;

public class RaceService {

	private MongoDatabase mongoDatabase;
	private MongoCollection<Document> drivers;
	private MongoCollection<Document> races;
	private MongoCollection<Document> trackinfo;
	private MongoCollection<Document> overallresults;
	private MongoCollection<Document> telemetry;

	public RaceService(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
		this.drivers = mongoDatabase.getCollection("driverinfo");
		this.races = mongoDatabase.getCollection("runinfo");
		this.overallresults = mongoDatabase.getCollection("overallresults");
		this.trackinfo = mongoDatabase.getCollection("trackinfo");
		this.telemetry = mongoDatabase.getCollection("telemetry");
	}

	// List all races
	public List<Document> getAll() {

		List<Document> docList1 = new ArrayList<Document>();

		Bson match = match(eq("run_command", "R"));
		Bson group = group("$race_id", first("event_name", "$event_name"), first("race_id", "$race_id"));
		Bson projection = project(fields(include("race_id", "event_name"), excludeId()));
		MongoCursor<Document> cursor1 = races.aggregate(asList(match, group, projection)).iterator();

		while (cursor1.hasNext()) {
			docList1.add(cursor1.next());
		}

		return docList1;

//		List<String> docList = new ArrayList<String>();
//		MongoCursor<String> files = drivers.distinct("race_class", String.class).iterator();
//		while (files.hasNext()) {
//			docList.add(files.next());
//		}
//		return docList;
	}

//	public List<Document> getAll1() {
//		List<Document> docList = new ArrayList<Document>();
//		MongoCursor<Document> files = drivers.find().projection(Projections.include("race_class", "race_id"))
//				.iterator();
//		while (files.hasNext()) {
//			docList.add(files.next());
//			if (files.equals(files.tryNext())) {
//				continue;
//			}
//		}
//		return docList;
//	}

	// Distinct Drivers given race name | Get Set of drivers for race
	public List<String> getdriverbyRace(String racename) {

		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = drivers.distinct("driver_name", Filters.eq("race_class", racename), String.class)
				.iterator();
		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}

	// Get the ranks of the race
	public List<Document> getRanks(String racename) {

		List<Document> docList = new ArrayList<Document>();
		MongoCursor<Document> files = overallresults.find(Filters.eq("race_class", racename))
				.projection(Projections.include("overall_rank", "team", "driver_id", "date")).iterator();

		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}

	public Document getRacemetadata(String raceid) {

		Document files = races.find(Filters.eq("race_id", raceid))
				.projection(Projections.include("event_name", "event_round", "run_name", "event_start_datetime"))
				.first();
		return files;
	}

	// Get a snapshot of a race at given time. (Including speed, rpm, anomaly
	// scores, etc of each car at the given time.
//	public List<Document> snapshot(String timestmp) {
//		List<Document> docList = new ArrayList<Document>();
//		MongoCursor<Document> files = telemetry.find(Filters.regex("time_of_day", timestmp)).iterator();
//		while (files.hasNext()) {
//			docList.add(files.next());
//		}
//		return docList;
//	}

//	public List<Document> snapshot(String timestmp, String raceid) {
//		
//		List<Document> docList = new ArrayList<Document>();
//		
//		FindIterable<Document> fi = telemetry.find(Filters.and(Filters.regex("time_of_day", timestmp),Filters.eq("race_id",raceid)));        
//        MongoCursor<Document> files = fi.iterator();
//        
//        while (files.hasNext()) {
//			docList.add(files.next());
//		}
//		return docList;
//	}

	public List<Document> snapshot(String timestmp, String raceid) {

		List<Document> docList = new ArrayList<Document>();
		Bson match = match(and(Filters.regex("time_of_day", timestmp), eq("race_id", raceid)));
		Bson sort = sort(descending("car_num"));
		Bson group = group("$car_num", first("car_num", "$car_num"), first("vehicle_speed", "$vehicle_speed"),
				first("engine_rpm", "$engine_rpm"), first("throttle", "$throttle"));
		Bson projection = project(fields(include("car_num", "vehicle_speed", "engine_rpm", "throttle"), excludeId()));
		MongoCursor<Document> files = telemetry.aggregate(asList(match, sort, group, projection)).iterator();

		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}

	// Get all the flags of a given race.
	public List<String> getflagsbyRace(String racename) {
		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = overallresults
				.distinct("flag_status", Filters.eq("race_class", racename), String.class).iterator();
		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}
}
