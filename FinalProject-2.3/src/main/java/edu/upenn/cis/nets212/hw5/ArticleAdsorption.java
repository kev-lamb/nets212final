package edu.upenn.cis.nets212.hw5;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import com.google.gson.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.time.LocalDate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.json4s.jackson.*;

import edu.upenn.cis.nets212.config.Config;
import edu.upenn.cis.nets212.storage.DynamoConnector;
import edu.upenn.cis.nets212.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class ArticleAdsorption {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(ArticleAdsorption.class);

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;
	
	Table users;
	
	Table friends;
	
	Table news;
	
	Table likes;
	
	Table recommended;
	
	// A static graph that will be constructed from the static News DynamoDB table.
	JavaPairRDD<Tuple2<Node, Node>, Double> articleCategoryGraph; 
	
	public ArticleAdsorption() {
		System.setProperty("file.encoding", "UTF-8");
	}
	
	/**
	 * Creates and initializes the News, Likes, and Recommended DynamoDB tables invoked in 2.3.
	 * 
	 * @throws DynamoDbException
	 * @throws InterruptedException
	 */
	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			news = db.createTable("News", Arrays.asList(new KeySchemaElement("link", KeyType.HASH)),																				     
					Arrays.asList(new AttributeDefinition("link", ScalarAttributeType.S), 
							new AttributeDefinition("likes", ScalarAttributeType.S), 
							new AttributeDefinition("category", ScalarAttributeType.S), 
							new AttributeDefinition("headline", ScalarAttributeType.S), 
							new AttributeDefinition("authors", ScalarAttributeType.S), 
							new AttributeDefinition("short_description", ScalarAttributeType.S), 
							new AttributeDefinition("date", ScalarAttributeType.S)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier

			news.waitForActive();
		} catch (final ResourceInUseException exists) {
			news = db.getTable("News");
		}
		
		try {
			likes = db.createTable("Likes", 
					Arrays.asList(new KeySchemaElement("UserID", KeyType.HASH), 
							new KeySchemaElement("link", KeyType.RANGE)), 																			         
					Arrays.asList(new AttributeDefinition("UserID", ScalarAttributeType.S),
							new AttributeDefinition("link", ScalarAttributeType.S)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier

			likes.waitForActive();
		} catch (final ResourceInUseException exists) {
			likes = db.getTable("Likes");
		}
		
		try {
			recommended = db.createTable("Recommended", 
					Arrays.asList(new KeySchemaElement("User_ID", KeyType.HASH), 
							new KeySchemaElement("link", KeyType.RANGE)), 																			         
					Arrays.asList(new AttributeDefinition("User_ID", ScalarAttributeType.S),
							new AttributeDefinition("link", ScalarAttributeType.S)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier
			recommended.waitForActive();
		} catch (final ResourceInUseException exists) {
			recommended = db.getTable("Recommended");
		}

	}

	/**
	 * Initialize the spark and database connections.
	 * Create and initialize the News, Likes, and Recommended DynamoDB tables.
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void initialize() throws IOException, InterruptedException {
		logger.info("Connecting to Spark...");		
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();		
		logger.debug("Connected to Spark!");
		
		
		logger.info("Connecting to DynamoDB...");	
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);	
		logger.debug("Connected to DynamoDB!");
		
		
		users = db.getTable("Users");
		friends = db.getTable("Friends");
		initializeTables(); // Assigns references to the news, likes, and recommended Table fields. 	
	}
	
	/**
	 * ****This method should not be called more than once.****
	 * 
	 * Populate the News DynamoDB table, and create the static articleCategoryGraph 
	 * from the static News DynamoDB table, in accordance with the following guidelines:
	 * 
	 * - category nodes for every news article category
	 * - article nodes connected to their corresponding category nodes (and vice versa)
	 * - for every category node c: all (c,a) edges have equal weights which sum to 0.5. 
	 * - for every article node a: all (a,c) edges have equal weights which sum to 0.5. 
	 * 
	 * ***The filtering by date will occur within the getNewsFeedNetwork() method.***
	 * 
	 * Sets the "articleCategoryGraph" field equal to a JavaPairRDD <Tuple2<from_node, to_node>, edge_weight>
	 * graph of the News table, consisting of (a, c) and (c, a) nodes. The edge weights will be
	 * initialized properly in accordance with the guidelines in getNewsFeedNetwork().
	 * @throws IOException
	 */
	void getAllArticleCategoryData(String filePath) throws IOException {
		String line;
		BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
		JsonParser parser = new JsonParser();
		
		boolean status = true;
		
		List<String> jsonFileFields = new ArrayList<String>();
		jsonFileFields.add("link");
		jsonFileFields.add("category");
		jsonFileFields.add("headline");
		jsonFileFields.add("authors");
		jsonFileFields.add("short_description");
		jsonFileFields.add("date");
		
		// A list to be accumulated and soon converted to a PairRDD<Tuple2<Node, Node>, Double>
		// through a context.parallelizePairs() call.
		List<Tuple2<Tuple2<Node, Node>, Double>> listEdgeWithWeight = new ArrayList<>();;
		
		while (status) {
			line = reader.readLine();
			
			if (line == null) {
				status = false;
				break;
			}
			
			JsonElement jsonTree = parser.parse(line);
			JsonObject jsonObject = jsonTree.getAsJsonObject();
			
			Item item = new Item(); // The new Item to be added to the News table.
			
			Node articleNode = null;  // The new article Node to be added to the edge RDD.
			Node categoryNode = null; // The new category Node to be added to the edge RDD.
			
			for (int i = 0; i < 6; i++) {
				String currField = jsonFileFields.get(i);
				JsonPrimitive jsonPrimitive = jsonObject.getAsJsonPrimitive(currField);
				String jsonFieldString = jsonPrimitive.getAsString();
				
				// currField.equals("link")
				if (i == 0) {		
					item.withPrimaryKey("link", jsonFieldString);
					
					articleNode = new Node(jsonFieldString, "article");
				}
				
				// currField.equals("category")
				if (i == 1) {
					item.withString("category", jsonFieldString);
					
					categoryNode = new Node(jsonFieldString, "category");
				}
				
				// currField.equals("headline")
				if (i == 2) {
					item.withString("headline", jsonFieldString);
				}

				// currField.equals("authors")
				if (i == 3) {
					item.withString("authors", jsonFieldString);
				}				
				
				// currField.equals("short_description")
				if (i == 4) {
					item.withString("short_description", jsonFieldString);
				}
				
				// currField.equals("date")
				if (i == 5) {
					item.withString("date", jsonFieldString);
					
					articleNode.publishDate = jsonFieldString;
				}							
			}
			
			// Add the new Item to the News table.
			news.putItem(item);
			
			// Create two Tuple2<Node, Node>, Double> values to represent the symmetric
			// edges along with an edge_weight (a placeholder 1.0). Add them to the accumulated list.
			
			Tuple2<Tuple2<Node, Node>, Double> directionOne = new Tuple2<Tuple2<Node, Node>, Double>
				(new Tuple2<Node, Node>(articleNode, categoryNode), 1.0);
			
			Tuple2<Tuple2<Node, Node>, Double> directionTwo = new Tuple2<Tuple2<Node, Node>, Double>
			(new Tuple2<Node, Node>(categoryNode, articleNode), 1.0);
			
			listEdgeWithWeight.add(directionOne);
			listEdgeWithWeight.add(directionTwo);
			
		}
		
		reader.close();
		
		// Convert listEdgeWithWeight to a PairRDD.
		JavaPairRDD<Tuple2<Node, Node>, Double> articleCategoryGraph = context.parallelizePairs(listEdgeWithWeight);
		
		// TODO: Properly initialize the edge weights (in accordance with the guidelines).
		// (I.e., no more placeholder "1.0" edge weights.)
		
		this.articleCategoryGraph = articleCategoryGraph;
	}
	
	/**
	 * Fetch the news feed data from the S3 path, and the user/article data from the DynamoDB tables,
	 * and create a directed edge list PairRDD with:
	 * - article nodes for articles published on the input <date> or earlier
	 * - category nodes for every news article category
	 * - article nodes connected to their corresponding category nodes (and vice versa)
	 * - for every category node c: all (c,a) edges have equal weights which sum to 0.5. 
	 * - for every article node a: all (a,c) edges have equal weights which sum to 0.5.
	 *  
	 * ^^^getAllArticleCategoryData() performs all the above EXCEPT the filtering by date.^^^
	 * ** This method will perform the filtering by date. ** 
	 * 
	 * Additionally:
	 * - user nodes for each user 
	 * - user nodes connected to their friends (and vice versa)
	 * - user nodes connected to the category nodes that they are interested in (vice versa)
	 * - user nodes connected to the article nodes that they have liked (vice versa)
	 * - for every user node u: 
	 *     - all (u,u') edges have equal weights which sum to 0.3. 
	 *     - all (u,c) edges have equal weights which sum to 0.3.
	 *     - all (u,a) edges have equal weights which sum to 0.4.
	 * 
	 * (The input <date> should be provided in "YYYY-MM-DD" format. Additionally, four years should 
	 * be added to each article's publish date when interpreting it.) 
	 * 
	 * @param filePath
	 * @param date
	 * @return JavaPairRDD: <Tuple2<from_node, to_node>, edge_weight>
	 */
	JavaPairRDD<Tuple2<Node, Node>, Double> getNewsFeedNetwork(String filePath, String date) {
		// TODO: Complete this method. (Invoke getAllArticleCategoryData(), Query the user-related tables.)
		
		
		// *Ensure that each Node initially has a LabelMap mapping each user to 0.0.* 
		// (both postIter and midIter), *unless* the node is a user node, where then
		// postIterLabelMap should map the user's username to 1.0.
		
		// Additionally, set the publishDate and link fields for any article node created.
		
		// Lists to be accumulated and soon converted to multiple PairRDD<Tuple2<Node, Node>, Double>'s
		// through context.parallelizePairs() calls.
		List<Tuple2<Tuple2<Node, Node>, Double>> userUserWeightList = new ArrayList<>();
		List<Tuple2<Tuple2<Node, Node>, Double>> userCategoryWeightList = new ArrayList<>();
		List<Tuple2<Tuple2<Node, Node>, Double>> userArticleWeightList = new ArrayList<>();
	    
		// TODO: Are these proper scans??
		
		// Scan the Friends table to create (u, u') edges.
		ItemCollection<ScanOutcome> friendsResults = friends.scan(new ScanSpec());
		Iterator<Item> itemIter = friendsResults.iterator();
		
		 while (itemIter.hasNext()) {
             Item item = itemIter.next();
             String userOneName = item.getString("User");
             String userTwoName = item.getString("Friend");
             
             Node userOneNode = new Node(userOneName, "user");
             Node userTwoNode = new Node(userTwoName, "user");
             
            // Edge weights initially given 1.0 as a placeholder. 
 			Tuple2<Tuple2<Node, Node>, Double> directionOne = new Tuple2<Tuple2<Node, Node>, Double>
				(new Tuple2<Node, Node>(userOneNode, userTwoNode), 1.0);
			
			Tuple2<Tuple2<Node, Node>, Double> directionTwo = new Tuple2<Tuple2<Node, Node>, Double>
			(new Tuple2<Node, Node>(userTwoNode, userOneNode), 1.0);
			
			userUserWeightList.add(directionOne);
			userUserWeightList.add(directionTwo);
         }
		
		// Scan the Users table to create (u, c) and (c, u) edges.
		ItemCollection<ScanOutcome> usersResults = users.scan(new ScanSpec());
		itemIter = usersResults.iterator();
		
		 while (itemIter.hasNext()) {
             Item item = itemIter.next();
             String username = item.getString("User");
             Node userNode = new Node(username, "user");
             
             List<String> interestList = item.getList("List of Interests");
             
             Iterator<String> interestListIter = interestList.iterator();
             while (interestListIter.hasNext()) {
            	 String categoryName = interestListIter.next();
            	 Node categoryNode = new Node(categoryName, "category");
            	
            	// Edge weights initially given 1.0 as a placeholder. 
      			Tuple2<Tuple2<Node, Node>, Double> directionOne = new Tuple2<Tuple2<Node, Node>, Double>
     				(new Tuple2<Node, Node>(userNode, categoryNode), 1.0);
     			
     			Tuple2<Tuple2<Node, Node>, Double> directionTwo = new Tuple2<Tuple2<Node, Node>, Double>
     			(new Tuple2<Node, Node>(categoryNode, userNode), 1.0); 
     			
     			userCategoryWeightList.add(directionOne);
     			userCategoryWeightList.add(directionTwo);
             }
         }
		
		// Scan the Likes table to create (u, a) and (a, u) edges.
		ItemCollection<ScanOutcome> likesResults = likes.scan(new ScanSpec());
		itemIter = likesResults.iterator();
		
		 while (itemIter.hasNext()) {
             Item item = itemIter.next();
             String username = item.getString("User");
             String link = item.getString("link");
             
             Node userNode = new Node(username, "user");
             Node articleNode = new Node(link, "article");
             
         	// Edge weights initially given 1.0 as a placeholder. 
   			Tuple2<Tuple2<Node, Node>, Double> directionOne = new Tuple2<Tuple2<Node, Node>, Double>
  				(new Tuple2<Node, Node>(userNode, articleNode), 1.0);
  			
  			Tuple2<Tuple2<Node, Node>, Double> directionTwo = new Tuple2<Tuple2<Node, Node>, Double>
  			(new Tuple2<Node, Node>(articleNode, userNode), 1.0); 
  			
  			userArticleWeightList.add(directionOne);
  			userArticleWeightList.add(directionTwo);
         }
		
		 JavaPairRDD<Tuple2<Node, Node>, Double> userUserGraph = context.parallelizePairs(userUserWeightList);
		 JavaPairRDD<Tuple2<Node, Node>, Double> userCategoryGraph = context.parallelizePairs(userCategoryWeightList);
		 JavaPairRDD<Tuple2<Node, Node>, Double> userArticleGraph = context.parallelizePairs(userArticleWeightList);
		
		// TODO: Properly initialize the edge weights (in accordance with the guidelines).
		// (I.e., no more placeholder "1.0" edge weights.)
		
		
		 // union call at the very end with everything and the articleCategoryGraph field.
		
		return null;
	}

	/**
	 * Main functionality in the program: read and process the network
	 * 
	 * @param dMax If the maximum change in the user score, considering any 
	 *             LabelMap, falls at or below this value, the algorithm will terminate. 
	 * @param iMax The maximum number of absorption iterations allowed to run before
	 *             the algorithm terminates. 
	 * @param debug If true, outputs the user score of each node after each iteration. 
	 * @param inputUser The user score of particular interest for the algorithm (i.e., the user 
	 *             whose news feed will be updated through the use of this algorithm). 
	 * @param date The date of the articles that will be considered when creating the
	 *             user's updated news feed, provided in "YYYY-MM-DD" format.
	 * @return The recommended article Node for the <inputUser>.
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public Node run(double dMax, int iMax, boolean debug, String inputUser, String date) throws IOException, InterruptedException {
		logger.info("Running");

		// Load the network.
		JavaPairRDD<Tuple2<Node, Node>, Double> network = getNewsFeedNetwork(Config.NEWS_FEED_PATH, date);
		
		// .filter() to clear out edges with bad-dated articles??
		
		/*
		 * Conduct the adsorption algorithm on the network.
		 */
		
		// A JavaRDD of the nodes of the graph is all that is needed to store each 
		// node's user scores (which are stored as a LabelMap field within each Node object).
		JavaRDD<Node> fromNodes = network.map(pair -> pair._1()._1()).distinct();
		JavaRDD<Node> toNodes = network.map(pair -> pair._1()._2()).distinct();
		JavaRDD<Node> nodes = fromNodes.union(toNodes).distinct();
		
        // Determine the source nodes. Note that their user scores will stay constant 
		// throughout the algorithm (in the case of a non-user node, 0.0 for all 
		// user scores), thus their user scores may be freely ignored. 
		JavaRDD<Node> sourceNodes = nodes.subtract(toNodes);
		
		// The non-source nodes will be the ones with user scores we care about.
		// This is the variable we will continue to update with the most current
		// postIterLabelMaps for each of its nodes. 
		JavaRDD<Node> nonSourceNodes = nodes.subtract(sourceNodes);
		
		// (Variable defined outside the scope of the upcoming while loop merely so it may be 
		// invoked after the while loop. Irrelevant value at the moment.)
		JavaRDD<Node> updatedNonSourceNodes = nonSourceNodes;
		
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
			// accumulating their midIterLabelMaps. 
			
			updatedNonSourceNodes = network.map(entry -> entry._1()._2());
				// Includes duplicates that we must coalesce.
			
			
			// We will coalesce through a call to aggregateByKey(), where the value we aggregate
			// are the midIterLabelMaps of each non-source node. We first make these midIterLabelMaps
			// the value of a PairRDD<Node, Map>.
			JavaPairRDD<Node, Map<String, Double>> nonsourceNodeAndLabelMap = updatedNonSourceNodes
					.mapToPair(node -> new Tuple2<Node, Map<String, Double>>(node, node.midIterLabelMap));
			
			JavaPairRDD<Node, Map<String, Double>> coalesced = nonsourceNodeAndLabelMap
					.aggregateByKey(new HashMap<String, Double>(), 
							(val, row) -> (new coalesceSeqOrCombFunc()).call(val, row),      // Seq
							(val1, val2) -> (new coalesceSeqOrCombFunc()).call(val1, val2)); // Comb
			
			// (At this point, no more duplicate nodes are present.)
			
			// We now pass the accumulated midIterLabelMaps to their corresponding non-source node.
			// (I.e., updating the internal fields of each node.)
			coalesced.foreach(pair -> {
				Node node = pair._1;
				node.midIterLabelMap = pair._2;
			});
			
			// We now extract the Node key of the coalesced PairRDD<Node, Map>.
			updatedNonSourceNodes = coalesced.map(pair -> pair._1);
			
			// Now finalize the postIterLabelMaps for each node in updatedNonSourceNodes.
			updatedNonSourceNodes.foreach(node -> {
				node.finalizeLabelMap();
			});
			
			// TODO: Have a debug mode with printed information every iteration? (Optional)
			/*
			if (debug) {
			
			}
			*/
			
			/*
			 * Test for convergence by comparing nonSourceNodes and updatedNonSourceNodes.
			 */
			
			// A PairRDD created to facilitate an upcoming join().
			JavaPairRDD<Node, Node> convenientPairOne = updatedNonSourceNodes
					.mapToPair(node -> new Tuple2<Node, Node>(node, node));
			
			// A PairRDD created to facilitate an upcoming join().
			JavaPairRDD<Node, Node> convenientPairTwo = nonSourceNodes
					.mapToPair(node -> new Tuple2<Node, Node>(node, node));
			
			JavaPairRDD<Node, Tuple2<Node, Node>> joinRDD = convenientPairOne.join(convenientPairTwo);
			
			JavaPairRDD<Node, Node> newNodeAndOldNode = joinRDD
					.mapToPair(entry -> new Tuple2<Node, Node>(entry._2._1, entry._2._2));
			
			JavaPairRDD<Node, Double> nodeAndScoreDiff = newNodeAndOldNode
					.mapToPair(pair -> new Tuple2<Node, Double>(pair._1,
							pair._1.postIterLabelMap.get(inputUser)
							- pair._2.postIterLabelMap.get(inputUser)));

			List<Tuple2<Double, Node>> maxScoreDiffList = nodeAndScoreDiff
					.mapToPair(pair -> new Tuple2<Double, Node>(pair._2, pair._1))
					.sortByKey(false)
					.take(1);
			
			double maxScoreDiff = maxScoreDiffList.get(0)._1();
			
			if (maxScoreDiff <= dMax) {
				break;
			}	
			
			/*
			 * Prepare for the upcoming iteration.
			 */
			 			
			// If a from_node in the network is also a non-source node, it should be updated 
			// to how it appears in updatedNonSourceNodes (to have an up-to-date postIterLabelMap
			// for propagating in the upcoming iteration).
			
			JavaRDD<Node> updatedNonSrcFromNodes = updatedNonSourceNodes.subtract(toNodes);
			
			// Recall: The source nodes are always exclusively from_nodes. A from_node is 
			// any node that appears in the 1st Node column in the edge network <Node, Node>.
			JavaRDD<Node> updatedFromNodes = updatedNonSrcFromNodes.union(sourceNodes);
			
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
				
			// We can now pass nonSourceNodes to its updated value. 
			// (To refer back to it in the upcoming iteration, when testing for convergence.)
			nonSourceNodes = updatedNonSourceNodes;
			
			iterationCount += 1;
		}
		
		/*
		 * TODO: Take the resulting updatedNonSourceNodes and compile the nodes that are type = "article" 
		 * with publishDate = <date>, disregarding the articles that were already recommended to the 
		 * <inputUser>. Among the remaining articles, randomly select one, using probabilities equal 
		 * to their scaled <inputUser> scores from each node's postIterLabelMap.
		 */

		JavaRDD<Node> todaysArticles = updatedNonSourceNodes
				.filter(node -> node.type.equals("article") && node.publishDate.equals(date));
		
		// TODO: Filter out the articles that were already recommended to the <inputUser>.
		// (Query the Recommended Table. Perhaps create another RDD<Node> and use subtract().)
		JavaRDD<Node> validArticles = todaysArticles;
		
		// Extract the <inputUser> score from each article's postIterLabelMap.
		JavaPairRDD<Node, Double> articleAndUserScore = validArticles
				.mapToPair(node -> new Tuple2<Node, Double>(node, node.postIterLabelMap.get(inputUser)));
		
		// Scale the <inputUser> scores such that they sum to 1.
		// 1) First obtain the sum of all <inputUser> scores across the nodes.
		JavaRDD<Double> userScores = articleAndUserScore.map(pair -> pair._2);
		
		Double scoreSum = userScores.aggregate(0.0, (val, row) -> val + row, (val1, val2) -> val1 + val2);
		
		// 2) Next scale each <inputUser> score by (1 / scoreSum).
		JavaPairRDD<Node, Double> articleAndScaledScore = articleAndUserScore
				.mapToPair(entry -> new Tuple2<Node, Double>(entry._1, entry._2 / scoreSum));
		
		// Randomly select and output an article using the scaled <inputUser> scores as weights.
		// 1) Represent the PairRDD<Node, Double> as a list of Pair<Node, Double> objects.
		// (Required by the eventually-invoked EnumeratedDistribution object.)
		JavaRDD<Pair<Node, Double>> asPairs = articleAndScaledScore
				.map(entry -> new Pair<>(entry._1, entry._2));
		
		List<Pair<Node, Double>> asPairList = asPairs.collect();
		
		// 2) Complete the weighted selection using an EnumeratedDistribution object.
		Node selectedNode = new EnumeratedDistribution<Node>(asPairList).sample();
		
		// TODO: Upload the link of selectedNode to the Recommended Table.
			
		logger.info("*** Finished adsorption algorithm! ***");
		
		return selectedNode;
	}


	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");

		if (spark != null)
			spark.close();
		
		DynamoConnector.shutdown();
	}
	
	// Private class whose call() method is invoked in the aggregateByKey() method 
	// that coalesces the nodes in updatedNonSourceNodes. Serves as the seq or comb function.
	// (See JavaDocs for clarification.)
	private class coalesceSeqOrCombFunc implements Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>> {
		
		// Sum the two maps, value-by-value, into a new map.
		public Map<String, Double> call(Map<String, Double> val, Map<String, Double> row) {
			if (val.isEmpty()) {
				// The empty map serves as our zeroValue in the aggregateByKey() call.
				// It is to be interpreted as a map from each user to the double 0.0.
				return row;
			} else {
				// A non-trivial sumMap:
				HashMap<String, Double> sumMap = new HashMap<>();
				
				Iterator<Entry<String, Double>> iter = row.entrySet().iterator();
				while (iter.hasNext()) {
					Entry<String, Double> entry = iter.next();
					String user = entry.getKey();
					Double score = entry.getValue();
					
					Double valMapScore = val.get(user);
		            if (valMapScore != null) {
		            	sumMap.put(user, score + valMapScore);
		            } else {
		            	// The valMapScore may be interpreted as 0.0.
		            	// (An absence of a <User, Score> entry in any LabelMap is
		            	// equivalent to a mapping of <User, 0.0>.)
		            	sumMap.put(user, score);
		            }	
				}
				
				// We must now consider the user entries that appear in the valMap
				// but do NOT appear in the rowMap.
				
				iter = val.entrySet().iterator();
				while (iter.hasNext()) {
					Entry<String, Double> entry = iter.next();
					String user = entry.getKey();
					Double score = entry.getValue();
					
					Double rowMapScore = row.get(user);
		            if (rowMapScore == null) {
		            	sumMap.put(user, score);
		            } 
				}
				
				return sumMap;				
			}
		}
		
	}
	
	/**
	 * A helper class to define each node type (user, category, article) all under a single object.
	 * 
	 * @author nets212
	 */
	private class Node {
		
		// "user", "category", or "article".
		private String type;
		
		// <Username>, <Category>, or <Link>.
		private String name;
		
		// Set to a date with format "YYYY-MM-DD" iff type = "article".
		private String publishDate = "N/A";
		
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
			
			if (type.equals("user")) {
				postIterLabelMap.put(name, 1.0);
			}
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
		// each edge in the Graph RDD.)
		private void addLabelMap(Map<String, Double> inputMap) {
			
			Iterator<Entry<String, Double>> inputIter = inputMap.entrySet().iterator();
			while (inputIter.hasNext()) {
				Entry<String, Double> inputEntry = inputIter.next();
				String user = inputEntry.getKey();
				Double inputScore = inputEntry.getValue();
				
				Double currScore = this.midIterLabelMap.get(user);
				if (currScore != null) {
					this.midIterLabelMap.put(user, currScore + inputScore);		
				} else {
					// "this" Node had not heard of the user until this point!
					// Update the midIterLabelMap to include an entry <user, inputScore>.
					// (I.e., currScore was effectively 0.0 beforehand.)
					this.midIterLabelMap.put(user, inputScore);	
				}
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
			Iterator<Entry<String, Double>> iter = midIterLabelMap.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Double> entry = iter.next();
				String user = entry.getKey();
				Double entryScore = entry.getValue();
				postIterLabelMap.put(user, entryScore);
			}
			
			// Re-zero the values of midIterLabelMap.
			zeroLabelMap(); // Unnecessary??
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
	
}
