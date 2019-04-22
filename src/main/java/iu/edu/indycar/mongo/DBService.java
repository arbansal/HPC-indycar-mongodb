package iu.edu.indycar.mongo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.util.JSON;

//import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

//All the database calls should be hidden inside service classes. Build this as a separate module so this can be imported at any other layer of indycar 
//application(Storm bolts, Dashboard Server etc) and call methods to retrieve necessary data. 
//We will be using your implementation as the data API for indycar project. 

public class DBService {
	
	//connection configs. Will include username, password, url etc
	private Config config;
	
	public DBService(Config config) {
		this.config = config;
	}
        
//		DB dB = (new MongoClient("localhost", 27017)).getDB("indycar");
//		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
//		boolean flag = false;
//		while(!flag) flag = authenticate(dB, bufferedReader);
//	}
//	
	public DriversService getDriversService() {
		return new DriversService(this.database);
	}
	
	public RaceService getRaceService() {
		return new RaceService(this.database);
	}
	
	public TrackService getTrackService() {
		return new TrackService(this.database);
	}
	
	public void mongodbconnect() {
		String client_url = "mongodb://" + this.config.getUsername() + ":" + this.config.getPassword() + "@" + this.config.getUrl() + ":" + this.config.getPort_no() + "/" + this.config.getDbname();
        MongoClientURI uri = new MongoClientURI(client_url);
		
        MongoClient mongo_client = new MongoClient(uri);
        
        MongoDatabase db = mongo_client.getDatabase(this.config.getDbname());
        
        MongoCollection<Document> coll = db.getCollection(this.config.getCollectionnames());
        log.info("Fetching all documents from the collection");
        
     // Performing a read operation on the collection.
        FindIterable<Document> fi = coll.find();
        MongoCursor<Document> cursor = fi.iterator();
        try {
            while(cursor.hasNext()) {
                log.info(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        
	}
	
//	private static boolean authenticate(DB dB, BufferedReader bufferedReader) throws IOException {
//		
//		boolean flag = true;
//		System.out.println("User:");
//        String user = bufferedReader.readLine();
//        System.out.println("Password: ");
//        String password = bufferedReader.readLine();
//        if (dB.     authenticate(user, password.toCharArray())) {
//        	DBCollection channelDBCollection = dB.getCollection("trackinfo");
//        	String command = null;
//        	while(true) {
//        		System.out.println("What do you want to do?");
//        		command = bufferedReader.readLine();
//        		if(command.equals("exit")) break;
//        		else if (command.equals("findAll")) findAll(channelDBCollection);
//        	} 	
//        } else {
//        	System.out.println("Invalid username/password...");
//        	flag = false;
//        }
//        return flag;
//	}
//		DBService db = new DBService(config);
//    	
//        DriversService ds = db.getDriversService();
//    	MongoCollection<Document> collection = database.getCollection(collectionname);
//    	
//    	RaceService rs = db.getRaceService();
//    	MongoCollection<Document> collection = database.getCollection(collectionname);
//    	
//    	TrackService ts = db.getTrackService();
//    	MongoCollection<Document> collection = database.getCollection(collectionname);
        
	private static void findAll(DBCollection channelDBCollection) {
		DBCursor dbCursor = channelDBCollection.find();
		while(dbCursor.hasNext()) System.out.println(dbCursor.next());
	}
	
}
