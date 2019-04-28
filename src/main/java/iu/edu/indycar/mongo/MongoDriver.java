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
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
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
                document.append("race_id", Integer.toString(countr));
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
                
                entryDoc.append("race_id", Integer.toString(countr));
                entryDoc.append("car_num", splits[4]);
                entryDoc.append("uid", splits[5]);
                entryDoc.append("driver_name", splits[6]);
                entryDoc.append("race_class", splits[9]);
                entryDoc.append("driver_id", splits[10]);
                entryDoc.append("license", splits[13]);
                entryDoc.append("driver_team", splits[14]);
                entryDoc.append("driver_team_id", splits[15]);
                entryDoc.append("engine", splits[16]);
                entryDoc.append("driver_home_town", splits[18]);
                
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
                
                runinfoDoc.append("race_id", Integer.toString(countr));
                
                String hex = splits[2];
            	int value = Integer.parseInt(hex, 16);
            	String seq_num = Integer.toString(value);              
                
            	runinfoDoc.append("seq_num", seq_num);
                runinfoDoc.append("event_name", splits[4]);
                runinfoDoc.append("event_round", splits[5]);
                runinfoDoc.append("run_name", splits[6]);
                runinfoDoc.append("run_command", splits[7]);
                
                String HexString= splits[8];
                
                //reference: https://stackoverflow.com/questions/17432735/convert-unix-time-stamp-to-date-in-java
                long seconds=Long.parseLong(HexString,16);
                Date date1 = new java.util.Date(seconds*1000L);
                SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String formattedDate = sdf.format(date1);
                
                runinfoDoc.append("event_start_datetime", formattedDate);
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
                
                flaginfoDoc.append("race_id", Integer.toString(countr));
                
                String hex1 = splits[2];
            	int value1 = Integer.parseInt(hex1, 16);
            	String seq_num1 = Integer.toString(value1);
                
            	flaginfoDoc.append("seq_num", seq_num1);
                flaginfoDoc.append("track_status", splits[4]);
                
                String hex2 = splits[5];
            	int value2 = Integer.parseInt(hex2, 16);
            	String lap_nmbr = Integer.toString(value2);
                
                flaginfoDoc.append("lap_number", lap_nmbr);
                
                String HexString1= splits[6];
            	long unixSeconds1=Long.parseLong(HexString1,16);
            	int time1 = (int) (unixSeconds1/10000);
            	LocalTime timeOfDay = LocalTime.ofSecondOfDay(time1);
            	String greentime = timeOfDay.toString();
                
                flaginfoDoc.append("green_time", greentime);
                flaginfoDoc.append("green_laps", splits[7]);
                flaginfoDoc.append("yellow_time", splits[8]);
                flaginfoDoc.append("yellow_laps", splits[9]);
                flaginfoDoc.append("red_time", splits[10]);
                
                String hex3 = splits[11];
            	int value3 = Integer.parseInt(hex3, 16);
            	String num_of_yellows = Integer.toString(value3);
                
                flaginfoDoc.append("number_of_yellows", num_of_yellows);
                flaginfoDoc.append("current_leader_car", splits[12]);
                flaginfoDoc.append("no_of_lead_changes", splits[13]);
                flaginfoDoc.append("avg_race_speed", splits[14]);
                flaginfoDoc.append("date", date);
                                                
                if (flagRecords.size() == 100) {
                	flaginfo.insertMany(flagRecords);
                	flagRecords.clear();
                } else {
                	flagRecords.add(flaginfoDoc);
                }
            } else if(line.startsWith("$T")) {
            	String[] splits = line.split("¦");
                Document trackinfoDoc = new Document();
                
                trackinfoDoc.append("race_id", Integer.toString(countr));
                
                String hex1 = splits[2];
            	int value1 = Integer.parseInt(hex1, 16);
            	String seq_num1 = Integer.toString(value1);
                
            	trackinfoDoc.append("seq_num", seq_num1);
                trackinfoDoc.append("track_name", splits[4]);
                trackinfoDoc.append("venue", splits[5]);
                trackinfoDoc.append("track_length", splits[6]);
                trackinfoDoc.append("number_of_sections", splits[7]);
                trackinfoDoc.append("section_name", splits[8]);
                trackinfoDoc.append("section_length", splits[9]);
                trackinfoDoc.append("section_start_label", splits[10]);
                trackinfoDoc.append("section_end_label", splits[11]);
                
                if(splits[12]!=null) {
                	trackinfoDoc.append("section_2_name", splits[12]);
                    trackinfoDoc.append("section_2_length", splits[13]);
                    trackinfoDoc.append("section_2_start_label", splits[14]);
                    trackinfoDoc.append("section_2_end_label", splits[15]);
                }
                
                trackinfoDoc.append("date", date);
                                                
                if (trackRecords.size() == 100) {
                	trackinfo.insertMany(trackRecords);
                	trackRecords.clear();
                } else {
                	trackRecords.add(trackinfoDoc);
                }
            }else if(line.startsWith("$O")) {
            	String[] splits = line.split("¦");
                Document overallinfoDoc = new Document();
                
                overallinfoDoc.append("race_id", Integer.toString(countr));
                
                String hex1 = splits[2];
            	int value1 = Integer.parseInt(hex1, 16);
            	String seq_num1 = Integer.toString(value1);
            	
            	overallinfoDoc.append("seq_num", seq_num1);
            	
            	String hex2 = splits[4];
            	int value2 = Integer.parseInt(hex2, 16);
            	String res_id1 = Integer.toString(value2);
            	
                overallinfoDoc.append("result_ID", res_id1);
                
                String hex3 = splits[7];
            	int value3 = Integer.parseInt(hex3, 16);
            	String rank1 = Integer.toString(value3);
                
                overallinfoDoc.append("rank", rank1);
                
                String hex4 = splits[8];
            	int value4 = Integer.parseInt(hex4, 16);
            	String overall_rank1 = Integer.toString(value4);
                
                overallinfoDoc.append("overall_rank", overall_rank1);
                overallinfoDoc.append("start_position", splits[9]);
                overallinfoDoc.append("best_lap_time", splits[10]);
                overallinfoDoc.append("best_lap", splits[11]);
                
                String hex5 = splits[13];
            	int value5 = Integer.parseInt(hex5, 16);
            	String laps1 = Integer.toString(value5);
                
                overallinfoDoc.append("laps", laps1);
                overallinfoDoc.append("total_time", splits[14]);
                overallinfoDoc.append("total_qual_time", splits[20]);
                overallinfoDoc.append("diff", splits[22]);
                overallinfoDoc.append("gap", splits[23]);
                overallinfoDoc.append("pit_stops", splits[25]);
                overallinfoDoc.append("flag_status", splits[28]);
                overallinfoDoc.append("driver_name", splits[30] + " " + splits[31]);
                overallinfoDoc.append("race_class", splits[32]);
                overallinfoDoc.append("team", splits[35]);
                
                String hex6 = splits[37];
            	int value6 = Integer.parseInt(hex6, 16);
            	String t_drvr_pnts = Integer.toString(value6);
            	
                overallinfoDoc.append("total_driver_points", t_drvr_pnts);
                overallinfoDoc.append("driver_id", splits[49]);
                overallinfoDoc.append("qualifying_speed", splits[50]);
                overallinfoDoc.append("date", date);
                                                
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
                
                sectioninfoDoc.append("race_id", Integer.toString(countr));
                sectioninfoDoc.append("car_num", splits[4]);
                sectioninfoDoc.append("driver_id", driver_num);
                
                String hex = splits[5];
            	int value = Integer.parseInt(hex, 16);
            	String uid = Integer.toString(value);
                
                sectioninfoDoc.append("uid", uid);
                sectioninfoDoc.append("section_id", splits[6]);
                
                String HexString1= splits[7];
            	long unixSeconds1=Long.parseLong(HexString1,16);
            	int time1 = (int) (unixSeconds1/10000);
            	LocalTime timeOfDay = LocalTime.ofSecondOfDay(time1);
            	String elapsed_time = timeOfDay.toString();
                
                sectioninfoDoc.append("elapsed_time", elapsed_time);
                
                String HexString2= splits[7];
            	long unixSeconds2=Long.parseLong(HexString2,16);
            	int time2 = (int) (unixSeconds2/10000);
            	LocalTime timeOfDay1 = LocalTime.ofSecondOfDay(time2);
            	String last_section_time = timeOfDay1.toString();
                
                sectioninfoDoc.append("last_section_time", last_section_time);
                sectioninfoDoc.append("last_lap", splits[9]);
                sectioninfoDoc.append("date", date);
                                                
                if (sectionRecords.size() == 100) {
                	sectionresultsinfo.insertMany(sectionRecords);
                	sectionRecords.clear();
                } else {
                	sectionRecords.add(sectioninfoDoc);
                }
            
            }else if(line.startsWith("$W")) {

            	String[] splits = line.split("¦");
                Document weatherinfoDoc = new Document();
                
                weatherinfoDoc.append("race_id", Integer.toString(countr));
                weatherinfoDoc.append("time_of_day", splits[4]);
                weatherinfoDoc.append("ambient_temperature", splits[5]);
                weatherinfoDoc.append("relative_humidity", splits[6]);
                weatherinfoDoc.append("barometric_pressure", splits[7]);
                weatherinfoDoc.append("wind_speed", splits[8]);
                weatherinfoDoc.append("wind_direction", splits[9]);
                weatherinfoDoc.append("temperature_1", splits[12]);
                weatherinfoDoc.append("temperature_2", splits[14]);
                weatherinfoDoc.append("temperature_3", splits[16]);
                if(splits.length > 17) {weatherinfoDoc.append("temperature_4", splits[18]);}
                weatherinfoDoc.append("date", date);
                                                
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
