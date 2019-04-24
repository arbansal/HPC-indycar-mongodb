package iu.edu.indycar.mongo;

import org.bson.Document;

import java.sql.Time;
import java.util.Date;
import java.util.List;

public class MongoClientTest {
    public static void main(String[] args) {
        IndycarDBClient indycarDBClient = new IndycarDBClient("mongodb://localhost");
        
        List<Document> allDrivers = indycarDBClient.drivers().getAll();
        //System.out.println(allDrivers);
        
        List<String> allDrivers1 = indycarDBClient.drivers().getAll1();
        //System.out.println(allDrivers1);

        Document driver0 = indycarDBClient.drivers().getDriver("0");
        //System.out.println(driver0);
        
        Document driverprofile = indycarDBClient.drivers().getDriver1(0);
        //System.out.println(driverprofile);
        
        List<String> searchdriverbyname = indycarDBClient.drivers().search("st");
        //System.out.println(searchdriverbyname);
        
        List<String> getdriverbyrace = indycarDBClient.drivers().getDriversByRace("IndyCar");
        //System.out.println(getdriverbyrace);
        
        List<String> getallraces = indycarDBClient.races().getAll();
        //System.out.println(getallraces);
        
        List<String> getdriverbyRaceName = indycarDBClient.races().getdriverbyRace("IndyCar");
        //System.out.println(getdriverbyRaceName);
        
        List<Document> getranksbyRaceName = indycarDBClient.races().getRanks("IndyCar");
        //System.out.println(getranksbyRaceName);
        
        //Time myTime;
        Date mydate = new Date();
        
        List<Document> getsnapshot = indycarDBClient.races().snapshot(mydate);
        //System.out.println(getsnapshot);
        
        List<String> getflagsbyrace = indycarDBClient.races().getflagsbyRace("IndyCar");
        //System.out.println(getflagsbyrace);
        
        List<String> gettracks = indycarDBClient.tracks().getAll();
        //System.out.println(gettracks);
        
        List<Document> gettrackinfo = indycarDBClient.tracks().getTrack("Indianapolis Motor Speedway");
        System.out.println(gettrackinfo);
        
    }
}