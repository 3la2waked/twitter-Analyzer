package cs523.twitterAnalyzer;

/*
 * This is an entity class to hold the tweet object
 * simple setters and getters are included and an
 * overridden toString method to format this object
 * 
 * The most interesting part here is status field
 * which holds the processed data of what emotion
 * does this tweet hold
 */
public class Tweet {

	private String id;
	private String text;
	private String username;
	private String timestamp;
	private String status;
	
	public Tweet(String id, String text, String username, String timestamp, String status) {
		this.id = id;
		this.text = text;
		this.username = username;
		this.timestamp = timestamp;
		this.status = status;
	}
	
	public Tweet() {}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String toString() {
		return id + "\t" + text.replace("\n", " ") + "\t" + username + "\t" + timestamp + "\t" + status;
	}
}