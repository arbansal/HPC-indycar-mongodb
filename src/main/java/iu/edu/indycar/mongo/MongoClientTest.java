package iu.edu.indycar.mongo;

import org.bson.Document;
import java.util.List;

public class MongoClientTest {
	public static void main(String[] args) {
		IndycarDBClient indycarDBClient = new IndycarDBClient("mongodb://localhost");

		// DRIVERS QUERY 1 -- LIST ALL THE DRIVERS
		List<Document> allDrivers = indycarDBClient.drivers().getAll();
		// System.out.println(allDrivers);

		List<String> allDrivers1 = indycarDBClient.drivers().getAll1();
		// System.out.println(allDrivers1);

		// DRIVERS QUERY 2 -- SEARCH DRIVERS BASED ON CRITERIA (DriverID or DriverName)
		Document driver0 = indycarDBClient.drivers().getDriver("0");
		// System.out.println(driver0);

		List<String> searchdriverbyname = indycarDBClient.drivers().search("st");
		// System.out.println(searchdriverbyname);

		// DRIVERS QUERY 3 -- GET SET OF DRIVERS FOR RACE
		List<String> getdriverbyrace = indycarDBClient.drivers().getDriversByRace("IndyCar");
		// System.out.println(getdriverbyrace);

		// DRIVERS QUERY 4 -- GET THE DRIVER'S PROFILE
		Document driverprofile = indycarDBClient.drivers().getDriver1("326");
		// System.out.println(driverprofile);

		// DRIVERS QUERY 5 -- GET SECTION RECORDS WHEN DRIVER AND RACE IS GIVEN
		List<Document> getLapRecords = indycarDBClient.drivers().getLapRecords("1", "1026");
		// System.out.println(getLapRecords);

		// RACES QUERY 1 -- LIST ALL RACES (NAME AND ID)
		List<String> getallraces = indycarDBClient.races().getAll();
		// System.out.println(getallraces);

		List<Document> getallraces1 = indycarDBClient.races().getAll1();
		// System.out.println(getallraces1);

		// RACES QUERY -- GET DRIVER NAME by RACE NAME
		List<String> getdriverbyRaceName = indycarDBClient.races().getdriverbyRace("IndyCar");
		// System.out.println(getdriverbyRaceName);

		// RACES QUERY 2 -- GET ALL RACE METADATA
		Document getracemetadata = indycarDBClient.races().getRacemetadata("IndyCar");
		// System.out.println(getracemetadata);

		// RACES QUERY 3 -- GET ALL RANKS OF THE RACE
		List<Document> getranksbyRaceName = indycarDBClient.races().getRanks("IndyCar");
		// System.out.println(getranksbyRaceName);

		// RACES QUERY 4 -- GET THE SNAPSHOT OF RACE AT GIVEN TIME

		String mytime = "14:55:01";
		List<Document> getsnapshot = indycarDBClient.races().snapshot(mytime);
		// System.out.println(getsnapshot);

		// RACES QUERY 5 - GET ALL FLAGS OF A GIVEN RACE
		List<String> getflagsbyrace = indycarDBClient.races().getflagsbyRace("IndyCar");
		// System.out.println(getflagsbyrace);

		// TRACKS QUERY 1 - LIST ALL THE TRACKS
		List<String> gettracks = indycarDBClient.tracks().getAll();
		// System.out.println(gettracks);

		// TRACKS QUERY 2 - GET ALL DETAILS OF THE TRACK WHEN TRACK_NAME IS GIVEN
		List<Document> gettrackinfo = indycarDBClient.tracks().getTrack("Indianapolis Motor Speedway");
		// System.out.println(gettrackinfo);

	}
}