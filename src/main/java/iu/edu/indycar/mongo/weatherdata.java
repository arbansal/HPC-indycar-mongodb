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

public class weatherdata {

    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost"));
        MongoDatabase database = mongoClient.getDatabase("indycar");
        MongoCollection<Document> collection = database.getCollection("weatherinfo");

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
        	if (line.startsWith("$W")) {
                String[] splits = line.split("¦");
                String TimeofDay = splits[4];
                String AmbientTemperature = splits[5];
                String RelativeHumidity = splits[6];
                String BarometricPressure = splits[7];
                String WindSpeed = splits[8];
                String WindDirection = splits[9];
                String Temperature1 = splits[12];
                String Temperature2 = splits[14];
                String Temperature3 = splits[16];
                String Temperature4 = splits[18];
                
                Document document = new Document();
             	
                document.append("Time of Day", TimeofDay);
                document.append("Ambient Temperature", AmbientTemperature);
                document.append("Relative Humidity", RelativeHumidity);
                document.append("Barometric Pressure", BarometricPressure);
                document.append("Wind Speed", WindSpeed);
                document.append("Wind Direction", WindDirection);
                document.append("Temperature 1", Temperature1);
                document.append("Temperature 2", Temperature2);
                document.append("Temperature 3", Temperature3);
                document.append("Temperature 4", Temperature4);
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

