package cs523.twitterAnalyzer;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

/*
 * A simple java application to create HBase table once
 */
public class HBaseTweetFirst
{

	/*
	 * Table name and column families
	 */
	private static final String TABLE_NAME = "tweets";
	private static final String TInfo = "tweet-info";
	private static final String TGInfo = "general-info";

	public static void main(String... args) throws IOException
	{

		Configuration config = HBaseConfiguration.create();

		try {
			Connection connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();
					
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(TInfo).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(TGInfo).setCompressionType(Algorithm.NONE));

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println(" Done!");
		}
		catch (Exception x) {
			System.out.println(x);
		}
	}
}
