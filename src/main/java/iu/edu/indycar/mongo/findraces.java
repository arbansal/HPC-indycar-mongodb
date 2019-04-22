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
import java.util.ArrayList;

public class findraces {

	public static <E> void main(String[] args) {

		MongoClient client = MongoClients.create(new ConnectionString("mongodb://localhost")); 
		MongoDatabase database = client.getDatabase("indycar");
		MongoCollection<Document> collection = database.getCollection("overallresults");
		
		//System.out.println(collection.count());
		
		//FindIterable<Document> findIterable = collection.find(eq("status", "A"));
		
//		Block<Document> printBlock = new Block<Document>() {
//            @Override
//            public void apply(final Document document) {
//                System.out.println(document.toJson());
//            }
//       };
//      collection.find().projection(Projections.include("track_length", "number_of_sections")).forEach(printBlock);
		
//		DBObject obj = new BasicDBObject("race_name", new BasicDBObject("$eq", "Test"));
//		List<String> result = collection.distinct("driver_name", obj);
//       
//		MongoCursor<String> files = collection.distinct("driver_name", String.class).iterator();
//	    while(files.hasNext()) {
//	      System.out.println(files.next());
//	    }
//		Document myDoc = collection.find().first();
//		System.out.println(myDoc.toJson());
		
//		Document myDoc = collection.find(eq("car_num", "3")).first();
//		System.out.println(myDoc.toJson());
		
	       // Distinct Drivers given race name | Get Set of drivers for race
//      MongoCursor<String> files = collection.distinct("driver_name",Filters.eq("race_name","IndyCar"), String.class).iterator();
      
      // Search driver by name
//      MongoCursor<String> files = collection.distinct("driver_name",Filters.regex("driver_name","al","i"),String.class).iterator();
      
      // get driver's profile
//      MongoCursor<String> files = collection.distinct("driver_name",Filters.eq("driver_id","385"), String.class).iterator();
//    
//       MongoCursor<Document> files = collection.find(Filters.eq("Class", "IndyCar")).
//               projection(Projections.include("Overall Rank",
//               "Team","Driver ID","date")).iterator();
//       System.out.println(files);

		MongoCursor<String> files = collection.distinct("Flag Status",Filters.eq("Class","IndyCar"), String.class).iterator();
		while(files.hasNext()) {
	        System.out.println(files.next());
	      }
//		
//      while(files.hasNext()) {
//        System.out.println(files.next());
//      }
		
//		DBCursor cursor = database.getCollection("driverinfo").find(new BasicDBObject("race_name", "Test")).limit(2);
//		while(cursor.hasNext()) {
//			System.out.println(cursor);
//			//orders.add(new ProductOrder(cursor.next()));
//		}
	}

}
