package iu.edu.indycar.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MongoDriver {

    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost"));
        MongoDatabase database = mongoClient.getDatabase("indycar");
//        MongoCollection<Document> collection = database.getCollection("telemetry");

        getLogsList("C:\\Users\\ARPIT-LENOVO\\Desktop\\E566\\Project files\\Indytest").forEach( file -> {
            try {
//                writeToDB(file, collection);
            	writeToDB(file, database);
            } catch (IOException e) {
                System.out.println("Error in writing data of file : " + file.getAbsolutePath());
            }
        });
    }

    public static List<File> getLogsList(String folderPath) {
        File folder = new File(folderPath);
        File[] listOfFiles = folder.listFiles();

        if (listOfFiles == null) {
            return Collections.emptyList();
        }

        return Arrays.stream(listOfFiles)
                .filter(file -> file.getName().matches("IPBroadcaster_Input_\\d{4}-\\d{2}-\\d{2}_\\d+.log")).collect(Collectors.toList());
    }

//    public static void writeToDB(File file, MongoCollection<Document> collection) throws IOException {
    public static void writeToDB(File file, MongoDatabase database) throws IOException {
    	MongoCollection<Document> telemetry = database.getCollection("telemetry");
        MongoCollection<Document> drivers = database.getCollection("driverinfo");
        MongoCollection<Document> runinfo = database.getCollection("runinfo");
        MongoCollection<Document> trackinfo = database.getCollection("trackinfo");
        MongoCollection<Document> flaginfo = database.getCollection("flaginfo");
        MongoCollection<Document> overallresultsinfo = database.getCollection("overallresults");
        MongoCollection<Document> weatherinfo = database.getCollection("trackinfo");
        
        drivers.createIndex(Indexes.ascending("uid"), new IndexOptions().unique(true));
        runinfo.createIndex(Indexes.ascending("uid"), new IndexOptions().unique(true));
        
    	System.out.println("Parsing file " + file.getAbsolutePath());

        FileReader fis = new FileReader(file);

        String date = file.getName().split("_")[2];

        BufferedReader br = new BufferedReader(fis);
        String line = br.readLine();

//        List<Document> docs = new ArrayList<>();
        List<Document> telemetryRecords = new ArrayList<>();
        List<Document> entryRecords = new ArrayList<>();
        List<Document> runRecords = new ArrayList<>();
        List<Document> trackRecords = new ArrayList<>();
        List<Document> flagRecords = new ArrayList<>();
        List<Document> overallRecords = new ArrayList<>();
        List<Document> weatherRecords = new ArrayList<>();
        
        while (line != null) {
            if (line.startsWith("$P")) {
                String[] splits = line.split("¦");
                String carNumber = splits[1];
                String timeOfDay = splits[2];
                String lapDistance = splits[3];
                String vehicleSpeed = splits[4];
                String engineSpeed = splits[5];
                String throttle = splits[6];

                Document document = new Document();

                document.append("car_num", carNumber);
                document.append("lap_distance", lapDistance);
                document.append("time_of_day", timeOfDay);
                document.append("vehicle_speed", vehicleSpeed);
                document.append("engine_rpm", engineSpeed);
                document.append("throttle", throttle);
                document.append("date", date);

                if (telemetryRecords.size() == 100) {
                	telemetry.insertMany(telemetryRecords);
                	telemetryRecords.clear();
                } else {
                	telemetryRecords.add(document);
                }
            } else if(line.startsWith("$E")) {
            	String[] splits = line.split("¦");
                Document entryDoc = new Document();

                entryDoc.append("car_num", splits[4]);
                entryDoc.append("uid", splits[5]);
                entryDoc.append("driver_name", splits[6]);
                entryDoc.append("race_name", splits[9]);
                entryDoc.append("driver_id", splits[10]);
                entryDoc.append("license", splits[13]);
                entryDoc.append("driver_team", splits[14]);
                entryDoc.append("driver_team_id", splits[15]);
                entryDoc.append("engine", splits[16]);
                entryDoc.append("driver_home_town", splits[19]);
                
                drivers.updateOne(Filters.eq("uid", splits[5]),
                        new Document("$set", entryDoc), new UpdateOptions().upsert(true));
                
            } else if(line.startsWith("$R")) {
            	String[] splits = line.split("¦");
                Document runinfoDoc = new Document();

                runinfoDoc.append("event_name", splits[4]);
                runinfoDoc.append("event_round", splits[5]);
                runinfoDoc.append("run_name", splits[6]);
                runinfoDoc.append("run_command", splits[7]);
                runinfoDoc.append("event_start_datetime", splits[8]);
                runinfoDoc.append("date", date);
                                
//                runinfo.updateOne(Filters.eq("uid", splits[5]),
//                        new Document("$set", entryDoc), new UpdateOptions().upsert(true));
                if (runRecords.size() == 100) {
                	runinfo.insertMany(runRecords);
                	runRecords.clear();
                } else {
                	runRecords.add(runinfoDoc);
                }
            } else if(line.startsWith("$T")) {
            	String[] splits = line.split("¦");
                Document flaginfoDoc = new Document();

                flaginfoDoc.append("track_status", splits[4]);
                flaginfoDoc.append("lap_number", splits[5]);
                flaginfoDoc.append("green_time", splits[6]);
                flaginfoDoc.append("green_laps", splits[7]);
                flaginfoDoc.append("yellow_time", splits[8]);
                flaginfoDoc.append("yellow_laps", splits[9]);
                flaginfoDoc.append("red_time", splits[10]);
                flaginfoDoc.append("number_of_yellows", splits[11]);
                flaginfoDoc.append("current_leader", splits[12]);
                flaginfoDoc.append("no_of_lead_changes", splits[13]);
                flaginfoDoc.append("avg_race_speed", splits[14]);
                flaginfoDoc.append("date", date);
                                
//                flaginfoDoc.updateOne(Filters.eq("uid", splits[5]),
//                        new Document("$set", entryDoc), new UpdateOptions().upsert(true));
                
                if (flagRecords.size() == 100) {
                	trackinfo.insertMany(flagRecords);
                	flagRecords.clear();
                } else {
                	flagRecords.add(flaginfoDoc);
                }
            } else if(line.startsWith("$T")) {
            	String[] splits = line.split("¦");
                Document trackinfoDoc = new Document();

                trackinfoDoc.append("track_name", splits[4]);
                trackinfoDoc.append("venue", splits[5]);
                trackinfoDoc.append("track_length", splits[6]);
                trackinfoDoc.append("number_of_sections", splits[7]);
                trackinfoDoc.append("date", date);
                                
//                trackinfoDoc.updateOne(Filters.eq("uid", splits[5]),
//                        new Document("$set", entryDoc), new UpdateOptions().upsert(true));
                
                if (trackRecords.size() == 100) {
                	trackinfo.insertMany(trackRecords);
                	trackRecords.clear();
                } else {
                	trackRecords.add(trackinfoDoc);
                }
            }else if(line.startsWith("$O")) {
            	String[] splits = line.split("¦");
                Document overallinfoDoc = new Document();

                overallinfoDoc.append("ResultID", splits[4]);
                overallinfoDoc.append("Rank", splits[7]);
                overallinfoDoc.append("Overall Rank", splits[8]);
                overallinfoDoc.append("Start Position", splits[9]);
                overallinfoDoc.append("Best Lap Time", splits[10]);
                overallinfoDoc.append("Best Lap", splits[11]);
                overallinfoDoc.append("Laps", splits[13]);
                overallinfoDoc.append("Total Time", splits[14]);
                overallinfoDoc.append("Total Qual Time", splits[20]);
                overallinfoDoc.append("Diff", splits[22]);
                overallinfoDoc.append("Gap", splits[23]);
                overallinfoDoc.append("Pit Stops", splits[25]);
                overallinfoDoc.append("Flag Status", splits[28]);
                overallinfoDoc.append("First Name", splits[30]);
                overallinfoDoc.append("Last Name", splits[31]);
                overallinfoDoc.append("Class", splits[32]);
                overallinfoDoc.append("Team", splits[35]);
                overallinfoDoc.append("Total Driver Points", splits[37]);
                overallinfoDoc.append("Driver ID", splits[49]);
                overallinfoDoc.append("Qualifying Speed", splits[50]);
                overallinfoDoc.append("date", date);
                                
//                overallinfoDoc.updateOne(Filters.eq("uid", splits[5]),
//                        new Document("$set", entryDoc), new UpdateOptions().upsert(true));
                
                if (overallRecords.size() == 100) {
                	overallresultsinfo.insertMany(overallRecords);
                	overallRecords.clear();
                } else {
                	overallRecords.add(overallinfoDoc);
                }
            }
            line = br.readLine();
        }
        telemetry.insertMany(telemetryRecords);
        runinfo.insertMany(runRecords);
        flaginfo.insertMany(flagRecords);
        trackinfo.insertMany(trackRecords);
        overallresultsinfo.insertMany(overallRecords);
        br.close();
    }
}
