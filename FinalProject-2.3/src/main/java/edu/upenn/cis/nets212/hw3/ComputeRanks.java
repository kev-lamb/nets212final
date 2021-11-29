package edu.upenn.cis.nets212.hw3;

import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.upenn.cis.nets212.config.Config;
import edu.upenn.cis.nets212.storage.SparkConnector;
import scala.Tuple2;

public class ComputeRanks {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(ComputeRanks.class);

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	public ComputeRanks() {
		System.setProperty("file.encoding", "UTF-8");
	}

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void initialize() throws IOException, InterruptedException {
		logger.info("Connecting to Spark...");

		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		logger.debug("Connected!");
	}
	
	/**
	 * A helper class to define each node type (user, category, article) all under a single object.
	 * 
	 * @author nets212
	 */
	private class Node {
		
		// "user", "category", or "article".
		private String type;
		
		// <Username>, <Category>, <Headline>.
		private String name;
		
		// Set to a date with format "YYYY-MM-DD" iff type = "article".
		private String publishDate = "N/A";
		
		// Set to a URL iff type = "article".
		private String link = "N/A";
		
		// Map<Username, Score> held independently by each node (including the user nodes).
		// This holds the finalized LabelMap after an iteration of absorption is complete.
		private Map<String, Double> postIterLabelMap;
		
		// This holds the mid-processing LabelMap during the completion of an iteration.
		// After an iteration is complete, we will set postIterLabelMap -> midIterLabelMap,
		// and reset the values of midIterLabelMap to 0.0. 
		private Map<String, Double> midIterLabelMap;
		
		private Node(String name, String type) {
			this.name = name;
			this.type = type;
			
			this.postIterLabelMap = new HashMap<>();
			this.midIterLabelMap = new HashMap<>();
		}
		
		// *Scale the postIterLabelMap's values by an input edgeWeight, in a newly-created Map.*
		// (In preparation for propagating this scaled LabelMap to an out-neighbor Node.)
		private Map<String, Double> edgeScaleMap(double edgeWeight) {
			
			Map<String, Double> scaledMap = new HashMap<>();
			
			Iterator<Entry<String, Double>> iter = postIterLabelMap.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Double> entry = iter.next();
				String user = entry.getKey();
				Double entryScore = entry.getValue();
				scaledMap.put(user, entryScore * edgeWeight);
			}
			
			return scaledMap;
		}
		
		// *Add an input LabelMap to the midIterLabelMap; addition is value by value.*
		// (Called with an edgeScaledMap input from an in-neighbor as the input, for
		// each edge in the Graph RDD. Additionally, called towards the end of an iteration,
		// when coalescing each Node object (of potentially many) that represents the same
		// vertex in the graph, creating a single Node object for that vertex with a
		// single midIterLabelMap that is built by accumulation.)
		private void addLabelMap(Map<String, Double> inputMap) {
			
			Iterator<Entry<String, Double>> inputIter = inputMap.entrySet().iterator();
			while (inputIter.hasNext()) {
				Entry<String, Double> inputEntry = inputIter.next();
				String user = inputEntry.getKey();
				Double inputScore = inputEntry.getValue();
				
				Double currScore = midIterLabelMap.get(user);
				midIterLabelMap.put(user, currScore + inputScore);
			}
		}
		
		// *Normalize the user scores of the midIterLabelMap.* 
		// (Used at the conclusion of an iteration, on each coalesced Node object.)
		private void normalizeLabelMap() {
			
			// Sum the scores across all users.
			Double scoreSum = 0.0;
			
			Iterator<Entry<String, Double>> firstIter = midIterLabelMap.entrySet().iterator();
			while (firstIter.hasNext()) {
				Entry<String, Double> entry = firstIter.next();
				Double entryScore = entry.getValue();
				scoreSum += entryScore;
			}
			
			// Scale each score by (1 / scoreSum).
			Iterator<Entry<String, Double>> secondIter = midIterLabelMap.entrySet().iterator();
			while (secondIter.hasNext()) {
				Entry<String, Double> entry = secondIter.next();
				String user = entry.getKey();
				Double entryScore = entry.getValue();
				midIterLabelMap.put(user, entryScore / scoreSum);
			}
		}
		
		// *Zero the user scores of the midIterLabelMap.*
		// (Used on each to_node of the network to prepare for the next iteration.)
		// (Subtle difference between the manual re-zeroing in finalizeLabelMap().)
		private void zeroLabelMap() {
			Iterator<Entry<String, Double>> iter = midIterLabelMap.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Double> entry = iter.next();
				String user = entry.getKey();
				midIterLabelMap.put(user, 0.0);
			}
		}
		
		// *Accurately sets postIterLabelMap, and re-zeros the values of midIterLabelMap.* 
		// (Called at the conclusion of an iteration, on each coalesced Node object.) 
		private void finalizeLabelMap() {
			// Normalize the midIterLabelMap.
			normalizeLabelMap();
			
			// Set postIterLabelMap -> midIterLabelMap.
			postIterLabelMap = midIterLabelMap;
			
			// Re-zero the values of midIterLabelMap.
			midIterLabelMap = new HashMap<>();
			Iterator<Entry<String, Double>> iter = postIterLabelMap.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Double> entry = iter.next();
				String user = entry.getKey();
				midIterLabelMap.put(user, 0.0);
			}
		}
		
	    @Override
	    public boolean equals(Object o) {
	 
	        if (o == this) {
	            return true;
	        }

	        if (!(o instanceof Node)) {
	            return false;
	        }
	         
	        Node node = (Node) o;
	        
	        /*
	         * We don't bother to check for the equality of the LabelMaps: we will
	         * ensure that *every Node instance that intends to represent the same 
	         * vertex of the graph (i.e., there will be one Node instance for each
	         * time the vertex appears in the Graph RDD multiple times) has the 
	         * same postIterLabelMap at the end of each iteration.
	         * 
	         * (*): more on that on line __.
	         *  
	         * We determine if two Node instances represent the same vertex in the 
	         * graph through our definition of structural equality below:
	         */
	        return (this.name.equals(node.name) && this.type.equals(node.type));
	    }
	}
	
	/**
	 * Fetch the news feed data from the S3 path, and create a directed edge list PairRDD with:
	 * - article nodes for articles published on the input <date> or earlier
	 * - category nodes for every news article category
	 * - article nodes connected to their corresponding category nodes (and vice versa)
	 * - for every category node c: all (c,a) edges have equal weights which sum to 0.5. 
	 * - for every article node a: all (a,c) edges have equal weights which sum to 0.5. 
	 * 
	 * The input <date> should be provided in "YYYY-MM-DD" format. Additionally, four years should 
	 * be added to each article's publish date when interpreting it. 
	 * 
	 * @param filePath
	 * @return JavaPairRDD: <Tuple2<from_node, to_node>, edge_weight>
	 */
	JavaPairRDD<Tuple2<Node, Node>, Double> getNewsFeedNetwork(String filePath, String date) {
		// TODO: Complete this method.
		
		/*
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().replace('\t', ' '))
				.map(line -> line.split(" "));
		*/
		
		// *Ensure that each Node initially has a LabelMap mapping each user to 0.0.* 
		// (Both postIter and midIter.)
		
		// Additionally, set the publishDate and link fields for any article node created.
		
		long numEdges = 0;
		long numNodes = 0;
		
		System.out.println("This graph contains " + String.valueOf(numNodes) + " nodes and "
				+ String.valueOf(numEdges) + " edges.");
		
		return null;
	}
	
	/**
	 * Fetch the user data from the DynamoDB tables, and create a directed edge list PairRDD with:
	 * - user nodes for each user 
	 * - user nodes connected to their friends (and vice versa)
	 * - user nodes connected to the category nodes that they are interested in (vice versa)
	 * - user nodes connected to the article nodes that they have liked (vice versa)
	 * - for every user node u: 
	 *     - all (u,u') edges have equal weights which sum to 0.3. 
	 *     - all (u,c) edges have equal weights which sum to 0.3.
	 *     - all (u,a) edges have equal weights which sum to 0.4.
	 * 
	 * @return JavaPairRDD: <Tuple2<from_node, to_node>, edge_weight>
	 */
	JavaPairRDD<Tuple2<Node, Node>, Double> getUserNetwork() {
		// TODO: Complete this method.
		
		/*
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().replace('\t', ' '))
				.map(line -> line.split(" "));
		*/
		
		// *Ensure that each Node initially has a LabelMap mapping each user to 0.0.* 
		// (both postIter and midIter), *unless* the node is a user node, where then
		// postIterLabelMap should map the user's username to 1.0.
		
		// Additionally, set the publishDate and link fields for any article node created.
		
		long numEdges = 0;
		long numNodes = 0;
		
		System.out.println("This graph contains " + String.valueOf(numNodes) + " nodes and "
				+ String.valueOf(numEdges) + " edges.");
		
		return null;
	}

	/**
	 * Main functionality in the program: read and process the network
	 * 
	 * @param dMax If the maximum change in the user score considering any 
	 *             particular node falls at or below this value, the algorithm will terminate. 
	 * @param iMax The maximum number of absorption iterations allowed to run before
	 *             the algorithm terminates. 
	 * @param debug If true, outputs the user score of each node after each iteration. 
	 * @param inputUser The user score of particular interest for the algorithm (i.e., the user 
	 *             whose news feed will be updated through the use of this algorithm). 
	 * @param date The date of the articles that will be considered when creating the
	 *             user's updated news feed, provided in "YYYY-MM-DD" format.
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public void run(double dMax, int iMax, boolean debug, String inputUser, String date) throws IOException, InterruptedException {
		logger.info("Running");

        // Load the news feed network (no user data).
		JavaPairRDD<Tuple2<Node, Node>, Double> noUsers = getNewsFeedNetwork(Config.NEWS_FEED_PATH, date);
		
		// Load the users network.
		JavaPairRDD<Tuple2<Node, Node>, Double> withUsers = getUserNetwork();
		
		// Combine into an overall network.
		JavaPairRDD<Tuple2<Node, Node>, Double> network = withUsers.union(noUsers).distinct();
		
		/*
		 * Conduct the adsorption algorithm on the network.
		 */
		
		// A JavaRDD of the nodes of the graph is all that is needed to store each 
		// node's user scores (which are stored as a LabelMap field within each Node object).
		JavaRDD<Node> fromNodes = network.map(pair -> pair._1()._1()).distinct();
		JavaRDD<Node> toNodes = network.map(pair -> pair._1()._2()).distinct();
		JavaRDD<Node> nodes = fromNodes.union(toNodes).distinct();
		
        // Determine the source nodes. Note that their user scores will stay constant 
		// throughout the algorithm (and in the case of a non-user node, 0.0 for each 
		// user score), thus their user scores may be freely ignored. 
		JavaRDD<Node> sourceNodes = nodes.subtract(toNodes);
		
		// The non-source nodes will be the ones with user scores we care about.
		// This is the variable we will continue to update with the most current
		// postIterLabelMaps for each of its nodes. 
		JavaRDD<Node> nonSourceNodes = nodes.subtract(sourceNodes);
		
		/*
		 * Iteratively update the user scores
		 */
		int iterationCount = 1;
		while (iterationCount <= iMax) {
			
			// Propagate the user scores from each from_node -> to_node,
			// scaling by the edge weights.
			network.foreach(entry -> {
				Node fromNode = entry._1()._1();
				Node toNode = entry._1()._2();
				Double edgeWeight = entry._2();
				
				Map<String, Double> edgeScaledMap = fromNode.edgeScaleMap(edgeWeight);
				toNode.addLabelMap(edgeScaledMap);
			});
			
			// Among the non-source nodes, coalesce each Node representing the same vertex,
			// calling addLabelMap() to accumulate the midIterLabelMaps. 
			
			JavaRDD<Node> updatedNonSourceNodes = network.map(entry -> entry._1()._2());
				// Includes duplicates that we will coalesce.
			
			/*
			 * TODO: Coalesce the similar nodes in updatedNonSourceNodes. Perhaps mapToPair 
			 * to a PairRDD<Node, midIterLabelMap> and then aggregateByKey?
			 */
			
			// Now finalize the postIterLabelMaps for each node in updatedNonSourceNodes.
			// (No more duplicate nodes are present.)
			updatedNonSourceNodes.foreach(node -> {
				node.finalizeLabelMap();
			});
			
			/*
			 * TODO: Calculate the change in user score among each node's LabelMap, for the <inputUser>.
			 * This will be useful for measuring convergence, and possibly terminating the algorithm early.
			 * 
			 * Compare and contrast the variables nonSourceNodes and updatedNonSourceNodes. 
			 * If determined that convergence has been reached, break out of the while loop.
			 */
			
			// If a from_node in the network is also a non-source node, it should be updated 
			// to how it appears in updatedNonSourceNodes (to have an up-to-date postIterLabelMap
			// for propagating in the upcoming iteration).
			
			JavaRDD<Node> updatedFromNodes = updatedNonSourceNodes.union(sourceNodes);
			
			// (Created to permit the upcoming join(). No new information.)
			// (Expresses updatedFromNodes as PairRDD to permit joining.)
			JavaPairRDD<Node, Node> convenientFromNodes = updatedFromNodes
					.mapToPair(node -> new Tuple2<Node, Node>(node, node));
			
			// (Created to permit the upcoming join(). No new information.)
			// (Expresses network more conveniently as <Node, Tuple2<Node, Double>>.)
			JavaPairRDD<Node, Tuple2<Node, Double>> convenientNetwork = network
					.mapToPair(entry -> new Tuple2<Node, Tuple2<Node, Double>>
					(entry._1._1, new Tuple2<Node, Double>(entry._1._2, entry._2)));
			
			JavaPairRDD<Node, Tuple2<Node, Tuple2<Node, Double>>> helperJoin = 
					convenientFromNodes.join(convenientNetwork);
			
			// Update the from_node's of the network to those of updatedFromNodes.
			// (In preparation for the next iteration.)
			network = helperJoin.mapToPair(entry -> new Tuple2<Tuple2<Node, Node>, Double>
					(new Tuple2<Node, Node>(entry._2._1, entry._2._2._1), entry._2._2._2));
			
			// Now re-zero the midIterLabelMaps of each to_node in the network.
			// (In preparation for the next iteration.)
			network.foreach(entry -> {
				Node toNode = entry._1()._2();
				toNode.zeroLabelMap();	
			});
									
			// TODO: Have a debug mode with printed information every iteration?
			/*
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
			*/
				
			// We can now pass nonSourceNodes to its updated value. 
			// (To refer back to it in the upcoming iteration, when testing for convergence.)
			nonSourceNodes = updatedNonSourceNodes;
			
			// To preserve the value of iterationCount for console printing accuracy.
			if (iterationCount == iMax) {
				break;
			}
			
			iterationCount += 1;
		}
		
		/*
		 * TODO: Take the resulting Nodes and their corresponding user scores, and compile 
		 * the nodes that are type = "article" with publishDate = <date>. Among them, randomly
		 * select an article, using probabilities equal to the <inputUser> score 
		 * from each node's postIterLabelMap.
		 */

		logger.info("*** Finished absorption algorithm! ***");
	}


	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");

		if (spark != null)
			spark.close();
	}
	
	
    // TODO: Update main method.
	public static void main(String[] args) {
		final ComputeRanks cr = new ComputeRanks();

		try {
			cr.initialize();
			if (args.length == 0) {
				cr.run(30.0, 15, false, "testUser", "2021-11-27");
			} else if (args.length == 1) {
				cr.run(Double.valueOf(args[0]), 15, false, "testUser", "2021-11-27");
			} else if (args.length == 2) {
				cr.run(Double.valueOf(args[0]), Integer.valueOf(args[1]), false, "testUser", "2021-11-27");
			} else {
				cr.run(Double.valueOf(args[0]), Integer.valueOf(args[1]), true, "testUser", "2021-11-27");
			}
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			cr.shutdown();
		}
	}

}
