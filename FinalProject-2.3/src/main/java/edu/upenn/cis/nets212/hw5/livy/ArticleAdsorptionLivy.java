package edu.upenn.cis.nets212.hw3.livy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import edu.upenn.cis.nets212.config.Config;

public class ComputeRanksLivy {
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		
		LivyClient client = new LivyClientBuilder()
				  .setURI(new URI("http://ec2-52-6-212-200.compute-1.amazonaws.com:8998/"))
				  .build();

		try {
			String jar = "target/nets212-hw3-0.0.1-SNAPSHOT.jar";
			
		  System.out.printf("Uploading %s to the Spark context...\n", jar);
		  client.uploadJar(new File(jar)).get();
		  
		  String sourceFile = Config.SOCIAL_NET_PATH;//.BIGGER_SOCIAL_NET_PATH;
		  //String sourceFile = Config.BIGGER_SOCIAL_NET_PATH;

		  System.out.printf("Running SocialRankJob with %s as its input...\n", sourceFile);
		  
		  List<MyPair<Integer,Double>> resultBackLinks = client.submit
				  (new SocialRankJob(true, sourceFile)).get();
		  System.out.println("With backlinks: " + resultBackLinks);
		  
		  List<MyPair<Integer,Double>> resultNoBackLinks = client.submit
				  (new SocialRankJob(false, sourceFile)).get();
		  System.out.println("Without backlinks: " + resultNoBackLinks);

		} finally {
		  client.stop(true);
		}
	}

}
