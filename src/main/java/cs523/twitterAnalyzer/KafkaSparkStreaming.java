package cs523.twitterAnalyzer;

import com.google.gson.Gson;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Main streaming application in which the job will persist and
 * listen to the messages sent from Kafka, this will also analyze
 * the tweets to produce the emotion out of that tweet's text
 */
public class KafkaSparkStreaming {

    public static void main(String... args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("TwitterStreamingApp").setMaster("local[*]");
        
        // Declaring the streaming context to check for new messages every second
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        Collection<String> topics = Collections.singletonList("tweets_topic");
        
        // The following parameters are Kafka connection specific
        // since we have initialized Kafka locally, host and port
        // are targeted to localhost and the default port to connect
        // to Kafka, group.id is mandatory
        Map<String, Object> kafkaParams = new HashMap<String,Object>();

        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test-consumer-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // This is the actual receiver of Kafka messages
        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));

        messages.foreachRDD(
            rdd -> rdd.foreach(
                a -> {
                    Tweet tweet = new Gson().fromJson(a.value(), Tweet.class);
                    //tweet.setStatus(KafkaSparkStreaming.getEmotion(tweet.getText()));
                    tweet.setStatus(KafkaSparkStreaming.generateStatus(tweet.getText()));
                    System.out.println(tweet.toString());
                    HBaseTweetTable.putTweet(tweet);
                }
            )
        );

        streamingContext.start();
        streamingContext.awaitTermination();
    }
    
    /*
     * There is a limitation on how many times we can hit this endpoint
     * so we don't call it anymore 
     */
    public static String getEmotion(String text) {
    	try {
    		HttpClient httpclient = HttpClients.createDefault();
        	HttpPost httppost = new HttpPost("https://api.promptapi.com/text_to_emotion");
        	httppost.addHeader("apikey", "dgvWzyO8AC0wPsH4FwP1CdnkL8RBM50B");
        	httppost.addHeader("Content-Type", "application/x-www-form-urlencoded");
        	

        	// Request parameters and other properties.
        	List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        	params.add(new BasicNameValuePair("data-urlencode", text));
        	httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

        	//Execute and get the response.
        	HttpResponse response = httpclient.execute(httppost);
        	HttpEntity entity = response.getEntity();

        	if (entity != null) {
        	    try (InputStream instream = entity.getContent()) {
        	        return instream.toString();
        	    }
        	}
    	}
    	catch (Exception e) {
    		
    	}
    	
    	return "";
    }
    
    public static String generateStatus(String tweet) {
    	List<String> emotions = new ArrayList<>(Arrays.asList("happy", "sad", "normal", "excited"));
    	return emotions.get((int)(Math.random() * emotions.size()));
    }
}