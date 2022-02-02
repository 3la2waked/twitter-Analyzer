package cs523.twitterAnalyzer;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/*
 * This class handles the insertion flow of the data
 * into the HBase table 'tweets'
 */
public class HBaseTweetTable {

	/*
	 * Table specific column families and the corresponding byte values
	 */
	private static final String TInfo = "tweet-info";
	private static final String TGInfo = "general-info";

	private static final byte[] TInfo_BYTES = TInfo.getBytes();
	private static final byte[] TGInfo_BYTES = TGInfo.getBytes();

	private static Table tweetsTable;
	
	/*
	 * This method receives a tweet object and persists
	 * it into HBase
	 */
	public static void putTweet(Tweet tweet) throws IOException {

		Configuration config = HBaseConfiguration.create();
		String tableName = "tweets";
		Connection connection = null;

		try {
			// initialize the connection to HBase
			connection = ConnectionFactory.createConnection(config);
			tweetsTable = connection.getTable(TableName.valueOf(tableName));

			// declare a put object, fill the data and then insert it into the table
			Put row = new Put(tweet.getId().getBytes());
			row.addColumn(TInfo_BYTES, "TweetDesc".getBytes(), tweet.getText().getBytes());
			row.addColumn(TGInfo_BYTES, "UserName".getBytes(), tweet.getUsername().getBytes());
			row.addColumn(TGInfo_BYTES, "TimeStamp".getBytes(), tweet.getTimestamp().getBytes());
			row.addColumn(TGInfo_BYTES, "Status".getBytes(), tweet.getStatus().getBytes());

			tweetsTable.put(row);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// terminate everything gracefully
				if (tweetsTable != null) {
					tweetsTable.close();
				}

				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}