package edu.upenn.cis.nets212.hw5.livy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.upenn.cis.nets212.config.Config;
import edu.upenn.cis.nets212.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class ArticleAdsorptionJob implements Job<List<MyPair<Integer,Double>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;

	private boolean useBacklinks;

	private String source;
	
	private int iMax = 25;
	
	private double dMax = 30.0;
	
	private boolean debug = false;
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws DynamoDbException 
	 */
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting to Spark...");
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		System.out.println("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().replace('\t', ' '))
				.map(line -> line.split(" "));
		
		// Create a (follower, followed) edge graph. 
		// (Which will *not* require flipping the edges of the input graph.)
		JavaPairRDD<Integer, Integer> FollowerAndNode = file
				.mapToPair(pairArr -> new Tuple2<Integer, Integer>
				(Integer.valueOf(pairArr[0]), Integer.valueOf(pairArr[1])))
				.distinct();
		
		long numEdges = FollowerAndNode.count();
		
		// Computing the number of nodes is less trivial as there may be nodes
		// featured in multiple edges or featured on only certain sides of the edges.
		JavaRDD<Integer> followers = FollowerAndNode.map(pair -> pair._1).distinct();
		JavaRDD<Integer> followed = FollowerAndNode.map(pair -> pair._2).distinct();
		long numNodes = followers.union(followed).distinct().count();
		
		System.out.println("This graph contains " + String.valueOf(numNodes) + " nodes and "
				+ String.valueOf(numEdges) + " edges.");
		
		return FollowerAndNode;
	}
	
	private JavaRDD<Integer> getSinks(JavaPairRDD<Integer,Integer> network) {
		// Obtain all nodes with an out-degree >= 1.
		JavaRDD<Integer> nodesNotSinks = network.keys().distinct();
		
		// Obtain all nodes in the network, including those with in-degree = 0.
		JavaRDD<Integer> followers = network.map(pair -> pair._1).distinct();
		JavaRDD<Integer> followed = network.map(pair -> pair._2).distinct();
		JavaRDD<Integer> nodes = followers.union(followed).distinct();
		
		// nodes - nodesNotSinks = nodesAreSinks
		JavaRDD<Integer> nodesAreSinks = nodes.subtract(nodesNotSinks);
		return nodesAreSinks;
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws DynamoDbException DynamoDB is unhappy with something
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public List<MyPair<Integer,Double>> run() throws IOException, InterruptedException {
		System.out.println("Running");

		// Load the social network
		// followed, follower
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(source);

		// Take the TO nodes
		// (Store as a PairRDD with an irrelevant value, -1, in preparation
		// for a join() with the reversedNetwork PairRDD.)
		JavaPairRDD<Integer, Integer> sinks = getSinks(network)
				.mapToPair(sink -> new Tuple2<Integer, Integer>(sink, -1));

		// Will the network be updated to include back-edges to the sinks?
		JavaPairRDD<Integer, Integer> updatedNetwork = null;
		
		// Add back-edges, creating an updated network (if useBacklinks == true).
        // (Edges from any sink to all nodes a part of its in-degree.)
		if (useBacklinks) {
			// Create a reversed network: (followed, follower). 
			JavaPairRDD<Integer, Integer> reversedNetwork = network
					.mapToPair(pair -> new Tuple2<Integer, Integer>(pair._2, pair._1));
			
			// Create a PairRDD of the form (sink, (in-neighbor, -1)).
			JavaPairRDD<Integer, Tuple2<Integer, Integer>> sinkAndInNeighbor = reversedNetwork.join(sinks);
			 
			JavaPairRDD<Integer, Integer> backEdgesFromSinks = sinkAndInNeighbor
					.mapToPair(joinPair -> new Tuple2<Integer, Integer>(joinPair._1, joinPair._2._1()));

			long numBackEdges = backEdgesFromSinks.count();
			System.out.println("Added " + String.valueOf(numBackEdges) + " backlinks.");
			
			updatedNetwork = network.union(backEdgesFromSinks);
		} else {
			updatedNetwork = network;
		}
		
		/*
		 * Conduct the SocialRank algorithm on the updatedNetwork.
		 */
		
		// Initialize all SocialRanks to one. 
		JavaRDD<Integer> followers = updatedNetwork.map(pair -> pair._1).distinct();
		JavaRDD<Integer> followed = updatedNetwork.map(pair -> pair._2).distinct();
		JavaRDD<Integer> nodes = followers.union(followed).distinct();
		
		JavaPairRDD<Integer, Double> socialRank = nodes
				.mapToPair(node -> new Tuple2<Integer, Double>(node, 1.0));
		
		// Store the source node SocialRanks (useful for later ensuring  
		// that their SocialRanks stay constant, as they should.)
		JavaPairRDD<Integer, Double> sourceNodeSRanks = nodes
				.subtract(followed)
				.mapToPair(node -> new Tuple2<Integer, Double>(node, 1.0));
		
		// Compute the out-degree of each node.
		JavaPairRDD<Integer, Integer> outDegrees = updatedNetwork
				.aggregateByKey(0, (val, row) -> val + 1, (val1, val2) -> val1 + val2);
		
		// Compute the "out-weight" of each node (1/N(j) for each node j).
		JavaPairRDD<Integer, Double> outWeights = outDegrees
				.mapToPair(pair -> new Tuple2<Integer, Double>(pair._1, 1 / Double.valueOf(pair._2)));
		
		// Iteratively update the SocialRanks
		int iterationCount = 1;
		while (iterationCount <= iMax) {
			// Compute the "contribution" offered from each node (r_j * 1/N(j) for each node j).
			JavaPairRDD<Integer, Tuple2<Double, Double>> helperJoinOne = outWeights.join(socialRank);
			JavaPairRDD<Integer, Double> contributions = helperJoinOne
					.mapToPair(joinPair -> 
					new Tuple2<Integer, Double>(joinPair._1, joinPair._2._1() * joinPair._2._2()));
			
			// Join contribution on the network graph.
			JavaPairRDD<Integer, Tuple2<Integer, Double>> helperJoinTwo = updatedNetwork.join(contributions);
			
			// A (node, update) RDD mapping each node in the graph to its new SocialRank value,
			// denoted as update, as if the decay factor d = 0.
			JavaPairRDD<Integer, Double> srUpdatesNoDecay = helperJoinTwo
					.mapToPair(joinPair -> new Tuple2<Integer, Double>(joinPair._2._1(), joinPair._2._2()))
					.aggregateByKey(0.0, (val, row) -> val + row,
							(val1, val2) -> val1 + val2);
					
			// Now account for a non-zero decay factor to determine the true srUpdates.
			JavaPairRDD<Integer, Double> srUpdatesDecay = srUpdatesNoDecay
					.mapToPair(pair -> new Tuple2<Integer, Double>(pair._1, pair._2 * (1 - 0.15) + 0.15));
			
			// Ensure that source nodes maintain their SocialRank 
			// (which will never change), and update the SocialRank RDD. 
			JavaPairRDD<Integer, Double> newSocialRank = srUpdatesDecay.union(sourceNodeSRanks);
						
			// If in debug mode, output the SocialRank RDD after every iteration.
			if (debug) {
				System.out.println("Iteration " + String.valueOf(iterationCount) + " is complete.");
				
				List<Tuple2<Integer, Double>> sRankList = newSocialRank.collect();
				for (Tuple2<Integer, Double> tuple : sRankList) {
					int nodeID = tuple._1();
					double sRank = tuple._2();
					
					System.out.println(String.valueOf(nodeID) + " | " + String.valueOf(sRank));
				}
				
				System.out.println("----------");
			}
			
			// Compute the maximum SocialRank difference for any particular node. 
			JavaPairRDD<Integer, Double> diffSRank = newSocialRank
					.join(socialRank)
					.mapToPair(joinPair -> 
					new Tuple2<Integer, Double>(joinPair._1,
							Math.abs(joinPair._2._1() - joinPair._2._2())));
			
			// We swap the elements of each pair in order to invoke sortByKey.
			List<Tuple2<Double, Integer>> maxDiff = diffSRank
					.mapToPair(pair -> new Tuple2<Double, Integer>(pair._2, pair._1))
					.sortByKey(false)
					.take(1);
			
			double maxDiffSocialRank = maxDiff.get(0)._1();
			
			// We can now pass socialRank to its updated value. 
			socialRank = newSocialRank;

			System.out.println("Iteration: " + String.valueOf(iterationCount)
					+ ", maxDiffSR: " + String.valueOf(maxDiffSocialRank)
					+ ", dMax: " + String.valueOf(dMax));
			
			// If the maximum change in the SocialRank considering any 
			// particular node falls at or below dMax, the algorithm will terminate. 
			if (maxDiffSocialRank <= dMax) {
				break;
			}	
			
			// To preserve the value of iterationCount for console printing accuracy.
			if (iterationCount == iMax) {
				break;
			}
			
			iterationCount += 1;
		}
		
		// Output the Top-10 SocialRanks.
		// In order to sort by the SocialRanks we first swap the elements of
		// each pair, sortByKey, then re-swap each pair's elements. 
		JavaPairRDD<Integer, Double> sRankSorted = socialRank
				.mapToPair(pair -> new Tuple2<Double, Integer>(pair._2, pair._1))
				.sortByKey(false)
				.mapToPair(pair -> new Tuple2<Integer, Double>(pair._2, pair._1));
		
		List<Tuple2<Integer, Double>> topTen = sRankSorted.take(10);
		
		// We will create (and output) an effective duplicate list,
		// just of a more convenient type to get sent back to the driver.
		List<MyPair<Integer, Double>> topTenDuplicate = new ArrayList<>();

		System.out.println("---Below are the top 10 SocialRanks after " 
				+ String.valueOf(iterationCount) 
				+ " iteration(s):---");
		System.out.println("Node ID | SocialRank");
		System.out.println("--------------------");
		for (Tuple2<Integer, Double> tuple : topTen) {
			int nodeID = tuple._1();
			double sRank = tuple._2();
			System.out.println(String.valueOf(nodeID) + " | " + String.valueOf(sRank));
			
			MyPair<Integer, Double> pair = new MyPair<>(nodeID, sRank);
			topTenDuplicate.add(pair);
		}
		
		System.out.println("*** Finished social network ranking! ***");
		
		return topTenDuplicate;
	}

	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		System.out.println("Shutting down");
	}
	
	public ArticleAdsorptionJob(boolean useBacklinks, String source) {
		System.setProperty("file.encoding", "UTF-8");
		
		this.useBacklinks = useBacklinks;
		this.source = source;
	}

	@Override
	public List<MyPair<Integer,Double>> call(JobContext arg0) throws Exception {
		initialize();
		return run();
	}

}
