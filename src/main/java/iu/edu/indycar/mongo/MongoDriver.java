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
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class MongoDriver {
	
	public static int countr = 1;
	
    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost"));
        MongoDatabase database = mongoClient.getDatabase("indycar");
//        MongoCollection<Document> collection = database.getCollection("telemetry");

        getLogsList("C:\\Users\\ARPIT-LENOVO\\Desktop\\E566\\Project files\\Indytest").forEach( file -> {
            try {
//                writeToDB(file, collection);
            	writeToDB(file, countr, database);
            	countr++;
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
    public static void writeToDB(File file, int countr, MongoDatabase database) throws IOException {
    	MongoCollection<Document> telemetry = database.getCollection("telemetry");
        MongoCollection<Document> drivers = database.getCollection("driverinfo");
        MongoCollection<Document> runinfo = database.getCollection("runinfo");
        MongoCollection<Document> trackinfo = database.getCollection("trackinfo");
        MongoCollection<Document> flaginfo = database.getCollection("flaginfo");
        MongoCollection<Document> overallresultsinfo = database.getCollection("overallresults");
        MongoCollection<Document> weatherinfo = database.getCollection("weatherinfo");
        MongoCollection<Document> sectionresultsinfo = database.getCollection("sectionresults");
        
        drivers.createIndex(Indexes.ascending("uid"), new IndexOptions().unique(true));
//        runinfo.createIndex(Indexes.ascending("uid"), new IndexOptions().unique(true));
        
        HashMap<Integer, String> hmap = new HashMap<Integer, String>();
        
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
        List<Document> sectionRecords = new ArrayList<>();
        
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
                document.append("race_id", countr);
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
                
                entryDoc.append("race_id", countr);
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
                
                int car_num;
                try {
                	car_num = Integer.parseInt(splits[4]);
                }
                catch (NumberFormatException e)
                {
                	car_num = 0;
                }
                
                hmap.put(car_num, splits[10]);
                
                drivers.updateOne(Filters.eq("uid", splits[5]),
                        new Document("$set", entryDoc), new UpdateOptions().upsert(true));
                
            } else if(line.startsWith("$R")) {
            	String[] splits = line.split("¦");
                Document runinfoDoc = new Document();
                
                runinfoDoc.append("race_id", countr);
                runinfoDoc.append("event_name", splits[4]);
                runinfoDoc.append("event_round", splits[5]);
                runinfoDoc.append("run_name", splits[6]);
                runinfoDoc.append("run_command", splits[7]);
                runinfoDoc.append("event_start_datetime", splits[8]);
                runinfoDoc.append("date", date);
                                
                if (runRecords.size() == 100) {
                	runinfo.insertMany(runRecords);
                	runRecords.clear();
                } else {
                	runRecords.add(runinfoDoc);
                }
            } else if(line.startsWith("$F")) {
            	String[] splits = line.split("¦");
                Document flaginfoDoc = new Document();
                
                flaginfoDoc.append("race_id", countr);
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
                	flaginfo.insertMany(flagRecords);
                	flagRecords.clear();
                } else {
                	flagRecords.add(flaginfoDoc);
                }
            } else if(line.startsWith("$T")) {
            	String[] splits = line.split("¦");
                Document trackinfoDoc = new Document();
                
                trackinfoDoc.append("race_id", countr);
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
                
                overallinfoDoc.append("race_id", countr);
                overallinfoDoc.append("resultID", splits[4]);
                overallinfoDoc.append("rank", splits[7]);
                overallinfoDoc.append("overall_rank", splits[8]);
                overallinfoDoc.append("start_position", splits[9]);
                overallinfoDoc.append("best_lap_time", splits[10]);
                overallinfoDoc.append("best_lap", splits[11]);
                overallinfoDoc.append("laps", splits[13]);
                overallinfoDoc.append("total_time", splits[14]);
                overallinfoDoc.append("total_qual_time", splits[20]);
                overallinfoDoc.append("diff", splits[22]);
                overallinfoDoc.append("gap", splits[23]);
                overallinfoDoc.append("pit_stops", splits[25]);
                overallinfoDoc.append("flag_status", splits[28]);
                overallinfoDoc.append("first_name", splits[30]);
                overallinfoDoc.append("last_name", splits[31]);
                overallinfoDoc.append("class", splits[32]);
                overallinfoDoc.append("team", splits[35]);
                overallinfoDoc.append("total_driver_points", splits[37]);
                overallinfoDoc.append("driver_id", splits[49]);
                overallinfoDoc.append("qualifying_speed", splits[50]);
                overallinfoDoc.append("date", date);
                                
//                overallinfoDoc.updateOne(Filters.eq("uid", splits[5]),
//                        new Document("$set", entryDoc), new UpdateOptions().upsert(true));
                
                if (overallRecords.size() == 100) {
                	overallresultsinfo.insertMany(overallRecords);
                	overallRecords.clear();
                } else {
                	overallRecords.add(overallinfoDoc);
                }
            }else if(line.startsWith("$S")) {

            	String[] splits = line.split("¦");
                Document sectioninfoDoc = new Document();
                
                int car_num1;
                try {
                	car_num1 = Integer.parseInt(splits[4]);
                }
                catch (NumberFormatException e)
                {
                	car_num1 = 0;
                }
                
                String driver_num= hmap.get(car_num1);
                
                sectioninfoDoc.append("race_id", countr);
                sectioninfoDoc.append("car_num", splits[4]);
                sectioninfoDoc.append("driver_id", driver_num);
                sectioninfoDoc.append("uid", splits[5]);
                sectioninfoDoc.append("section_id", splits[6]);
                sectioninfoDoc.append("elapsed_time", splits[7]);
                sectioninfoDoc.append("last_section_time", splits[8]);
                sectioninfoDoc.append("last_lap", splits[9]);
                sectioninfoDoc.append("date", date);
                                
//                overallinfoDoc.updateOne(Filters.eq("uid", splits[5]),
//                        new Document("$set", entryDoc), new UpdateOptions().upsert(true));
                
                if (sectionRecords.size() == 100) {
                	sectionresultsinfo.insertMany(sectionRecords);
                	sectionRecords.clear();
                } else {
                	sectionRecords.add(sectioninfoDoc);
                }
            
            }else if(line.startsWith("$W")) {

            	String[] splits = line.split("¦");
                Document weatherinfoDoc = new Document();
                
                weatherinfoDoc.append("race_id", countr);
                weatherinfoDoc.append("time_of_day", splits[4]);
                weatherinfoDoc.append("ambient_temperature", splits[5]);
                weatherinfoDoc.append("relative_humidity", splits[6]);
                weatherinfoDoc.append("barometric_pressure", splits[7]);
                weatherinfoDoc.append("wind_speed", splits[8]);
                weatherinfoDoc.append("wind_direction", splits[9]);
                weatherinfoDoc.append("temperature_1", splits[12]);
                weatherinfoDoc.append("temperature_2", splits[14]);
                weatherinfoDoc.append("temperature_3", splits[16]);
                weatherinfoDoc.append("temperature_4", splits[18]);
                weatherinfoDoc.append("date", date);
                                
//                overallinfoDoc.updateOne(Filters.eq("uid", splits[5]),
//                        new Document("$set", entryDoc), new UpdateOptions().upsert(true));
                
                if (weatherRecords.size() == 100) {
                	weatherinfo.insertMany(weatherRecords);
                	weatherRecords.clear();
                } else {
                	weatherRecords.add(weatherinfoDoc);
                }
            
            }
            line = br.readLine();
        }
        telemetry.insertMany(telemetryRecords);
        runinfo.insertMany(runRecords);
        flaginfo.insertMany(flagRecords);
        trackinfo.insertMany(trackRecords);
        overallresultsinfo.insertMany(overallRecords);
        sectionresultsinfo.insertMany(sectionRecords);
        weatherinfo.insertMany(weatherRecords);
        br.close();
    }
}
