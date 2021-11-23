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
	 * The path to the space-delimited social network data
	 */
	public static String SOCIAL_NET_PATH = "s3a://penn-cis545-files/twitter_combined.txt";
	
	public static String BIGGER_SOCIAL_NET_PATH = "s3a://penn-cis545-files/soc-LiveJournal1.txt";
	
	public static String LOCAL_SPARK = "local[*]";

	/**
	 * How many RDD partitions to use?
	 */
	public static int PARTITIONS = 5;
}
