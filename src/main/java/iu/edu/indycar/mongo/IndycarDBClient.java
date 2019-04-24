package iu.edu.indycar.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class IndycarDBClient {

    private MongoDatabase database;

    /**
     * With this constructor, indycar db will be selected by default
     *
     * @param url
     */
    public IndycarDBClient(String url) {
        MongoClient mongoClient = MongoClients.create(
                new ConnectionString(url)
        );
        this.database = mongoClient.getDatabase("indycar");
    }

    /**
     * With this constructor default db can be changed
     *
     * @param url
     * @param db
     */
    public IndycarDBClient(String url,
                           String db) {
        MongoClient mongoClient = MongoClients.create(
                new ConnectionString(url)
        );
        MongoDatabase database = mongoClient.getDatabase(db);
    }

    public DriversService drivers() {
        return new DriversService(this.database);
    }
    
    public RaceService races() {
        return new RaceService(this.database);
    }
    
    public TrackService tracks() {
        return new TrackService(this.database);
    }
}