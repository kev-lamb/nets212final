package edu.upenn.cis.nets212.config;

/**
 * Global configuration for NETS 212 homeworks.
 * 
 * A better version of this would read a config file from the resources,
 * such as a YAML file.  But our first version is designed to be simple
 * and minimal. 
 * 
 * @author zives
 *
 */
public class Config {

	/**
	 * The path to the news feed data
	 */
	//public static String NEWS_FEED_PATH = "target/News_Category_Dataset_v2.json";
	public static String NEWS_FEED_PATH = "target/shorter_news_category_dataset_v2.json";
	
	public static String LOCAL_SPARK = "local[*]";

	/**
	 * How many RDD partitions to use?
	 */
	public static int PARTITIONS = 5;
	
	/**
	 * If we set up a local DynamoDB server, where does it listen?
	 */
	public static int DYNAMODB_LOCAL_PORT = 8000;

	/**
	 * This is the connection to the DynamoDB server. 
	 */
	public static String DYNAMODB_URL = "https://dynamodb.us-east-1.amazonaws.com";
	//		"https://dynamodb.us-east-1.amazonaws.com";
	
	/**
	 * Do we want to use the local DynamoDB instance or a remote one?
	 * 
	 * If we are local, performance is really slow - so you should switch
	 * to the real thing as soon as basic functionality is in place.
	 */
	public static Boolean LOCAL_DB = false;
}
