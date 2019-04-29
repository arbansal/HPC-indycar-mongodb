package iu.edu.indycar.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import static java.util.Arrays.asList;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Projections.*;

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

	// List all the drivers
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

	// Search driver by name (typing Ca, should return Ed Carpenter, Carl, etc.)
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

		List<String> docList = new ArrayList<String>();
		MongoCursor<String> files = drivers
				.distinct("driver_name", Filters.eq("race_class", search_race_name), String.class).iterator();
		while (files.hasNext()) {
			docList.add(files.next());
		}
		return docList;
	}

	// Search driver by UID
	public Document getDriver(String uid) {
		return this.drivers.find(new Document("uid", uid)).first();
	}

	// Get the driver’s profile
	public Document getDriver1(String driverId) {
		Document files = drivers.find(Filters.eq("driver_id", driverId))
				.projection(Projections.include("driver_name", "driver_team", "driver_home_town")).first();
		return files;
	}

	// DRIVERS QUERY 5 -- GET SECTION RECORDS (from $O and $S) WHEN DRIVER AND
	// RACEID IS GIVEN
	public List<Document> getLapRecords(String race_id, String driverId) {

		List<Document> docList1 = new ArrayList<Document>();

		Bson match = match(and(eq("race_id", race_id), eq("driver_id", driverId)));
		Bson sort = sort(descending("laps"));
		Bson group = group("$laps", first("laps", "$laps"), first("rank", "$rank"),
				first("overall_rank", "$overall_rank"), first("best_lap", "$best_lap"),
				first("pit_stops", "$pit_stops"), first("flag_status", "$flag_status"));
		Bson projection = project(
				fields(include("laps", "rank", "overall_rank", "best_lap", "pit_stops", "flag_status"), excludeId()));
		MongoCursor<Document> cursor1 = overallresults.aggregate(asList(match, sort, group, projection)).iterator();

		Bson match1 = match(and(eq("race_id", race_id), eq("driver_id", driverId)));
		Bson sort1 = sort(descending("last_lap"));
		Bson group1 = group("$last_lap", first("last_lap", "$last_lap"), first("section_id", "$section_id"),
				first("elapsed_time", "$elapsed_time"), first("last_section_time", "$last_section_time"));
		Bson projection1 = project(
				fields(include("last_lap", "section_id", "elapsed_time", "last_section_time"), excludeId()));
		MongoCursor<Document> cursor2 = sectionresults.aggregate(asList(match1, sort1, group1, projection1)).iterator();

//		while(cursor234.hasNext() && cursor123.hasNext()) {
//			docList111.add(cursor123.next());
//			docList111.add(cursor234.next());
//	    }	
		while (cursor1.hasNext()) {
			docList1.add(cursor1.next());
		}
		while (cursor2.hasNext()) {
			docList1.add(cursor2.next());
		}
		return docList1;
	}
}