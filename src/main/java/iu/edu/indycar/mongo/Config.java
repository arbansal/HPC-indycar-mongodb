package iu.edu.indycar.mongo;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class Config {

	private String username = null;
	private String password = null;
	private String url = "localhost";
	private String dbname;
	int port_no = 27017;
	
	
	public int getPort_no() {
		return port_no;
	}
	public void setPort_no(int port_no) {
		this.port_no = port_no;
	}
	private String[] collectionnames = {"driverinfo", "trackinfo", "runinfo", "overallresults", "flaginfo", "telemetry", "weather"};
	
	public String[] getCollectionnames() {
		return collectionnames;
	}
	public void setCollectionnames(String[] collectionnames) {
		this.collectionnames = collectionnames;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() throws UnsupportedEncodingException {
		return URLEncoder.encode(this.password, "UTF-8");
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDbname() {
		return dbname;
	}
	public void setDbname(String dbname) {
		this.dbname = dbname;
	}
	
}
