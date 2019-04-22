package iu.edu.indycar.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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

public class overallresults {

    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost"));
        MongoDatabase database = mongoClient.getDatabase("indycar");
        MongoCollection<Document> collection = database.getCollection("overallresults");

        getLogsList("C:\\Users\\ARPIT-LENOVO\\Desktop\\E566\\Project files\\Indytest").forEach(file -> {
            try {
                writeToDB(file, collection);
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

    public static void writeToDB(File file, MongoCollection<Document> collection) throws IOException {
        System.out.println("Parsing file " + file.getAbsolutePath());

        FileReader fis = new FileReader(file);

        String date = file.getName().split("_")[2];

        BufferedReader br = new BufferedReader(fis);
        String line = br.readLine();

        List<Document> docs = new ArrayList<>();
        while (line != null) {
            if (line.startsWith("$O")) {
                String[] splits = line.split("¦");
                String resultID = splits[4];
                String rank = splits[7];
                String overallrank = splits[8];
                String startposition = splits[9];
                String bestlaptime = splits[10];
                String bestlap = splits[11];
                String laps = splits[13];
                String totaltime = splits[14];
                String totalqualtime = splits[20];
                String diff = splits[22];
                String gap = splits[23];
                String pitstops = splits[25];
                String flagstatus = splits[28];
                String firstname = splits[30];
                String lastname = splits[31];
                String classname = splits[32];
                String team = splits[35];
                String totaldriverpoints = splits[37];
                String driverID = splits[49];
                String qualifyingspeed = splits[50];
                
                Document document = new Document();

                document.append("ResultID", resultID);
                document.append("Rank", rank);
                document.append("Overall Rank", overallrank);
                document.append("Start Position", startposition);
                document.append("Best Lap Time", bestlaptime);
                document.append("Best Lap", bestlap);
                document.append("Laps", laps);
                document.append("Total Time", totaltime);
                document.append("Total Qual Time", totalqualtime);
                document.append("Diff", diff);
                document.append("Gap", gap);
                document.append("Pit Stops", pitstops);
                document.append("Flag Status", flagstatus);
                document.append("First Name", firstname);
                document.append("Last Name", lastname);
                document.append("Class", classname);
                document.append("Team", team);
                document.append("Total Driver Points", totaldriverpoints);
                document.append("Driver ID", driverID);
                document.append("Qualifying Speed", qualifyingspeed);
                document.append("date", date);
                
                if (docs.size() == 100) {
                    collection.insertMany(docs);
                    docs.clear();
                } else {
                    docs.add(document);
                }
            }
            line = br.readLine();
        }
        collection.insertMany(docs);
        br.close();
    }
}

