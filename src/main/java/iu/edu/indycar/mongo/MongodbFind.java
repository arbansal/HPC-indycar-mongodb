package iu.edu.indycar.mongo;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongodbFind {

	public static void main(String[] args) {

		MongoClient client = MongoClients.create(new ConnectionString("mongodb://localhost")); // new
																								// MongoClient("localhost",
																								// 27017);
		MongoDatabase database = client.getDatabase("indycar");
		MongoCollection<Document> collection = database.getCollection("trackinfo");
		List<Document> documents = (List<Document>) collection.find().into(new ArrayList<Document>());
		
		System.out.println(documents.size());
		
		for (Document document : documents) {
			System.out.println(document);
		}
	}
}
