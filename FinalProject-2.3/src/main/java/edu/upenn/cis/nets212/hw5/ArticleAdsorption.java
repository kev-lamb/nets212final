package edu.upenn.cis.nets212.hw5;

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
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import com.google.gson.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;

import java.time.LocalDate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

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
	// This graph is NOT filtered by any date: all possible edges are present.
	JavaPairRDD<Tuple2<Node, Node>, Double> articleCategoryGraph; 
	
	/**
	 * Dear outside user,
	 * 
	 * 1) Create an instance of ArticleAdsorption.
	 * 
	 * 2) Call initialize().
	 * 
	 * 3) Repeatedly call run(double dMax, int iMax, boolean debug, String inputUser, String date)
	 *    for as many times as you need to recommend an article to a user.
	 *    (run() will return a String of the URL link of the recommended article.)
	 *    (Per the instructions, set iMax = 15 iterations.)
	 *    
	 * 4) Finally, when done operating your server, call shutdown().
	 * 
	 */
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
		logger.info("Beginning initializeTables...");
		try {
			news = db.createTable("News", Arrays.asList(new KeySchemaElement("link", KeyType.HASH)),																				     
					Arrays.asList(new AttributeDefinition("link", ScalarAttributeType.S)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier

			news.waitForActive();
		} catch (final ResourceInUseException exists) {
			news = db.getTable("News");
		}
		logger.info("Created the News table.");
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
		logger.info("Created the Likes table.");
		try {
			recommended = db.createTable("Recommended", 
					Arrays.asList(new KeySchemaElement("UserID", KeyType.HASH), 
							new KeySchemaElement("link", KeyType.RANGE)), 																			         
					Arrays.asList(new AttributeDefinition("UserID", ScalarAttributeType.S),
							new AttributeDefinition("link", ScalarAttributeType.S)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier
			recommended.waitForActive();
		} catch (final ResourceInUseException exists) {
			recommended = db.getTable("Recommended");
		}
		logger.info("Created the Recommended table.");
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
		
		logger.info("Tables initialized successfully.");
		
		// Perform the single read of the news feed JSON data, 
		// assigning a value to the articleCategoryGraph field.
		getAllArticleCategoryData(Config.NEWS_FEED_PATH); 
	}
	
	/**
	 * ***This method should not be called more than once.****
	 * ***It is called automatically within initialize().***
	 * 
	 * Populates the News DynamoDB table, and creates the static articleCategoryGraph 
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
	 * initialized properly in accordance with the guidelines.
	 * @throws IOException
	 */
	void getAllArticleCategoryData(String filePath) throws IOException {
		
		logger.info("Beginning getAllArticleCategoryData (read of JSON data)...");
		
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
					
					// We must add four years to the JSON's listed publish date.
	
					LocalDate dateObj = LocalDate.parse(jsonFieldString).plusYears(4);
					String correctedDate = dateObj.toString();
					
					item.withString("date", correctedDate);
					articleNode.publishDate = correctedDate;
					
					// System.out.println("Here is a corrected date: " + correctedDate);
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
		
		/* 
		 * Properly initialize the edge weights (in accordance with the guidelines).
		 * (I.e., no more placeholder "1.0" edge weights.)
		 */
		
		// 1a) Select the edges <<from_node, to_node>, weight> where from_node.type = "article".
		// (Which, in this case, also guarantees that to_node.type = "category".)
		JavaPairRDD<Tuple2<Node, Node>, Double> fromArticle = articleCategoryGraph
				.filter(entry -> entry._1()._1().type.equals("article"));
		
		// 1b) Select the edges <<from_node, to_node>, weight> where from_node.type = "category".
		// (Which, in this case, also guarantees that to_node.type = "article".)
	    JavaPairRDD<Tuple2<Node, Node>, Double> fromCategory = articleCategoryGraph
	    		.filter(entry -> entry._1()._1().type.equals("category"));
	    
	    // 2) Express the PairRDD's more conveniently as PairRDD<from_node, <to_node, weight>>.
	    // (To prepare to aggregateByKey() on the from_node.)
	    JavaPairRDD<Node, Tuple2<Node, Double>> fromArticleBetter = fromArticle.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1._1, new Tuple2<Node, Double>(entry._1._2, entry._2)));
	    
	    JavaPairRDD<Node, Tuple2<Node, Double>> fromCategoryBetter = fromCategory.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1._1, new Tuple2<Node, Double>(entry._1._2, entry._2)));
	    
	    // 3) Calculate the out-degree of each from_node in each separate PairRDD.
	    JavaPairRDD<Node, Integer> articleOutDeg = fromArticleBetter
	    		.aggregateByKey(0, (val, row) -> val + 1, (val1, val2) -> val1 + val2);
	    
	    JavaPairRDD<Node, Integer> categoryOutDeg = fromCategoryBetter
	    		.aggregateByKey(0, (val, row) -> val + 1, (val1, val2) -> val1 + val2);
	    
	    // 4) Join, on from_node, the out-degree PairRDD's with their respective "better" PairRRD's.
	    JavaPairRDD<Node, Tuple2<Tuple2<Node, Double>, Integer>> articleJoin = fromArticleBetter.join(articleOutDeg);
	    JavaPairRDD<Node, Tuple2<Tuple2<Node, Double>, Integer>> categoryJoin = fromCategoryBetter.join(categoryOutDeg);
		
	    // 5a) Scale each <<article, category>, weight>'s edge weight by (0.5 / articleOutDeg). 
	    JavaPairRDD<Node, Tuple2<Node, Double>> fromArticleProperEdges = articleJoin.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1, new Tuple2<Node, Double>(entry._2._1._1, entry._2._1._2 * (0.5 / entry._2._2))));
	    
	    // 5b) Scale each <<category, article>, weight>'s edge weight by (0.5 / categoryOutDeg).   
	    JavaPairRDD<Node, Tuple2<Node, Double>> fromCategoryProperEdges = categoryJoin.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1, new Tuple2<Node, Double>(entry._2._1._1, entry._2._1._2 * (0.5 / entry._2._2))));
	    
	    // 6) Convert the PairRDD's back to the original format of <<from_node, to_node>, weight>.
	    JavaPairRDD<Tuple2<Node, Node>, Double> finalFromArticle = fromArticleProperEdges.mapToPair
	    		(entry -> new Tuple2<Tuple2<Node, Node>, Double>
	    		(new Tuple2<Node, Node>(entry._1, entry._2._1), entry._2._2));
	    
	    JavaPairRDD<Tuple2<Node, Node>, Double> finalFromCategory = fromCategoryProperEdges.mapToPair
	    		(entry -> new Tuple2<Tuple2<Node, Node>, Double>
	    		(new Tuple2<Node, Node>(entry._1, entry._2._1), entry._2._2));
	    
	    // 7) Union the PairRDD's to create the output PairRDD.
		this.articleCategoryGraph = finalFromArticle.union(finalFromCategory);
		
		
		JavaRDD<Node> fromNodes = this.articleCategoryGraph.map(entry -> entry._1._1);
		JavaRDD<Node> toNodes = this.articleCategoryGraph.map(entry -> entry._1._2);
		long numNodes = fromNodes.union(toNodes).distinct().count();
		
        long numEdges = articleCategoryGraph.count();
        
		System.out.println("The articleCategoryGraph contains " + String.valueOf(numNodes) + " nodes and "
				+ String.valueOf(numEdges) + " edges.");
		
		
		logger.info("getAllArticleCategoryData complete.");
	}
	
	/**
	 * Fetches the user/article data from the DynamoDB tables, and create a directed edge list PairRDD following:	  
	 * - user nodes for each user 
	 * - user nodes connected to their friends (and vice versa)
	 * - user nodes connected to the category nodes that they are interested in (vice versa)
	 * - user nodes connected to the article nodes that they have liked (vice versa)
	 * - for every user node u: 
	 *     - all (u,u') edges have equal weights which sum to 0.3. 
	 *     - all (u,c) edges have equal weights which sum to 0.3.
	 *     - all (u,a) edges have equal weights which sum to 0.4.
	 *     
	 * - for every category node c: all (c,u) edges have equal weights which sum to 0.5. 
	 * - for every article node a: all (a,u) edges have equal weights which sum to 0.5.
	 * 
	 * Unions this graph with the output graph of getAllArticleCategoryData() into a single
	 * PairRDD graph, and outputs the resulting PairRDD graph,
	 * which will be ready for processing by the adsorption algorithm.
	 * 
	 * **All article nodes in the resulting graph will be filtered by <date>.**
	 * (Only articles published on or before the input <date> will appear as nodes.) 
	 * 
	 * (The input <date> should be provided in "YYYY-MM-DD" format. Additionally, four years should 
	 * be added to each article's publish date when interpreting it.) 
	 * 
	 * @param filePath
	 * @param date Filters out the articles published after this date.(Provided in "YYYY-MM-DD" format.)
	 * @return JavaPairRDD: <Tuple2<from_node, to_node>, edge_weight> A culminated graph for the entire network.
	 * @throws IOException, DynamoDbException
	 */
	JavaPairRDD<Tuple2<Node, Node>, Double> getNewsFeedNetwork(String date) throws IOException, DynamoDbException {
        
		logger.info("Beginning getNewsFeedNetwork...");
		
		// Lists to be accumulated and soon converted to multiple PairRDD<Tuple2<Node, Node>, Double>'s
		// through context.parallelizePairs() calls.
		List<Tuple2<Tuple2<Node, Node>, Double>> userUserWeightList = new ArrayList<>();
		List<Tuple2<Tuple2<Node, Node>, Double>> userCategoryWeightList = new ArrayList<>();
		List<Tuple2<Tuple2<Node, Node>, Double>> userArticleWeightList = new ArrayList<>();
		
		// Scan the Friends table to create (u, u') edges.
		Map<String, String> expressionAttrNameMap = new HashMap<>();
		expressionAttrNameMap.put("#u", "user");
		
		ItemCollection<ScanOutcome> friendsResults = friends.scan(new ScanSpec()
				.withProjectionExpression("#u, friend")
				.withNameMap(expressionAttrNameMap));
		Iterator<Item> itemIter = friendsResults.iterator();
			
		while (itemIter.hasNext()) {
		    Item item = itemIter.next();
		    String userOneName = item.getString("user");
		    String userTwoName = item.getString("friend");
		     
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
		ItemCollection<ScanOutcome> usersResults = users.scan(new ScanSpec()
				.withProjectionExpression("username, list_of_interests"));
		itemIter = usersResults.iterator();
		
		while (itemIter.hasNext()) {
            Item item = itemIter.next();
            String username = item.getString("username");
            Node userNode = new Node(username, "user");
             
            List<String> interestList = item.getList("list_of_interests");
             
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
		expressionAttrNameMap = new HashMap<>();
		expressionAttrNameMap.put("#d", "date");
		
		ItemCollection<ScanOutcome> likesResults = likes.scan(new ScanSpec()
				.withProjectionExpression("UserID, link, #d")
				.withNameMap(expressionAttrNameMap));
		itemIter = likesResults.iterator();
		
		while (itemIter.hasNext()) {
            Item item = itemIter.next();
            String username = item.getString("UserID");
            String link = item.getString("link");
            String publishDate = item.getString("date");
             
            Node userNode = new Node(username, "user");
            Node articleNode = new Node(link, "article");
            articleNode.publishDate = publishDate;
             
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
		
		logger.info("Filtering out the articles based on the date parameter.");
		
		/*
		 * Filter the articleCategoryGraph and the userArticleGraph by <date>.
		 * If an article has a publish date after <date>, its associated edge should be removed.
		 * 
		 * (Add four years to any article publish date when interpreting it.)
		 */
		
		JavaPairRDD<Tuple2<Node, Node>, Double> articleCategoryFiltered = articleCategoryGraph.filter
				(entry -> 
				
				/*
				 * (new FilterOutAfterDate(date)).call(entry);
				 */
				
				{LocalDate currDateObj = LocalDate.parse(date);
				Node fromNode = entry._1._1;
				Node toNode = entry._1._2;
				
				// Test the fromNode.
				if (fromNode.type.equals("article")) {
					String publishDate = fromNode.publishDate;
					
					// We add four years to the publish date of an article when interpreting it
					// (per the instructions). 
					LocalDate nodeDateObj = LocalDate.parse(publishDate).plusYears(4);
					
					if (nodeDateObj.isAfter(currDateObj)) {
						return false;
					}
					
				}
				
				// Test the toNode.
				if (toNode.type.equals("article")) {
					String publishDate = toNode.publishDate;
					
					// We add four years to the publish date of an article when interpreting it
					// (per the instructions). 
					// LocalDate nodeDateObj = LocalDate.parse(publishDate).plusYears(4);
					LocalDate nodeDateObj = LocalDate.parse(publishDate);
					
					if (nodeDateObj.isAfter(currDateObj)) {
						return false;
					}
				}
				
				// All clear!
				return true;});
		
		JavaPairRDD<Tuple2<Node, Node>, Double> userArticleFiltered = userArticleGraph.filter
				(entry -> 
				
				/*
				 * (new FilterOutAfterDate(date)).call(entry);
				 */
				
				{LocalDate currDateObj = LocalDate.parse(date);
				Node fromNode = entry._1._1;
				Node toNode = entry._1._2;
				
				// Test the fromNode.
				if (fromNode.type.equals("article")) {
					String publishDate = fromNode.publishDate;
					
					// We add four years to the publish date of an article when interpreting it
					// (per the instructions). 
					//LocalDate nodeDateObj = LocalDate.parse(publishDate).plusYears(4);
					LocalDate nodeDateObj = LocalDate.parse(publishDate);
					
					if (nodeDateObj.isAfter(currDateObj)) {
						return false;
					}
					
				}
				
				// Test the toNode.
				if (toNode.type.equals("article")) {
					String publishDate = toNode.publishDate;
					
					// We add four years to the publish date of an article when interpreting it
					// (per the instructions). 
					LocalDate nodeDateObj = LocalDate.parse(publishDate).plusYears(4);
					
					if (nodeDateObj.isAfter(currDateObj)) {
						return false;
					}
				}
				
				// All clear!
				return true;});
		
		logger.info("Properly initializing the edge weights of the overall network.");
		
		/* 
		 * Properly initialize the edge weights (in accordance with the guidelines).
		 * (I.e., no more placeholder "1.0" edge weights.)
		 */
		
		// 1a) Select the edges <<from_node, to_node>, weight>
		// where from_node.type = "user", to_node.type = "user".
		JavaPairRDD<Tuple2<Node, Node>, Double> userToUser = userUserGraph; // Given.
																		    // Just hand it a nicer name.
		
		// 1b) Select the edges <<from_node, to_node>, weight>
		// where from_node.type = "user", to_node.type = "category".
		JavaPairRDD<Tuple2<Node, Node>, Double> userToCategory = userCategoryGraph
				.filter(entry -> entry._1()._1().type.equals("user"));
		
		// 1c) Select the edges <<from_node, to_node>, weight>
		// where from_node.type = "category", to_node.type = "user".
		JavaPairRDD<Tuple2<Node, Node>, Double> categoryToUser = userCategoryGraph
				.filter(entry -> entry._1()._1().type.equals("category"));
		
		// 1d) Select the edges <<from_node, to_node>, weight>
		// where from_node.type = "user", to_node.type = "article".
		JavaPairRDD<Tuple2<Node, Node>, Double> userToArticle = userArticleFiltered
				.filter(entry -> entry._1()._1().type.equals("user"));
		
		// 1e) Select the edges <<from_node, to_node>, weight>
		// where from_node.type = "article", to_node.type = "user".
		JavaPairRDD<Tuple2<Node, Node>, Double> articleToUser = userArticleFiltered
				.filter(entry -> entry._1()._1().type.equals("article"));
		
	    // 2) Express the PairRDD's more conveniently as PairRDD<from_node, <to_node, weight>>.
	    // (To prepare to aggregateByKey() on the from_node.)
	    JavaPairRDD<Node, Tuple2<Node, Double>> userToUserBetter = userToUser.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1._1, new Tuple2<Node, Double>(entry._1._2, entry._2)));
	    
	    JavaPairRDD<Node, Tuple2<Node, Double>> userToCategoryBetter = userToCategory.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1._1, new Tuple2<Node, Double>(entry._1._2, entry._2)));
	    
	    JavaPairRDD<Node, Tuple2<Node, Double>> categoryToUserBetter = categoryToUser.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1._1, new Tuple2<Node, Double>(entry._1._2, entry._2)));
	    
	    JavaPairRDD<Node, Tuple2<Node, Double>> userToArticleBetter = userToArticle.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1._1, new Tuple2<Node, Double>(entry._1._2, entry._2)));
	    
	    JavaPairRDD<Node, Tuple2<Node, Double>> articleToUserBetter = articleToUser.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1._1, new Tuple2<Node, Double>(entry._1._2, entry._2)));
	    
	    // 3) Calculate the out-degree of each from_node in each separate PairRDD.
	    JavaPairRDD<Node, Integer> userOutDegToUser = userToUserBetter
	    		.aggregateByKey(0, (val, row) -> val + 1, (val1, val2) -> val1 + val2);
	    
	    JavaPairRDD<Node, Integer> userOutDegToCategory = userToCategoryBetter
	    		.aggregateByKey(0, (val, row) -> val + 1, (val1, val2) -> val1 + val2);
	    
	    JavaPairRDD<Node, Integer> categoryOutDegToUser = categoryToUserBetter
	    		.aggregateByKey(0, (val, row) -> val + 1, (val1, val2) -> val1 + val2);
	    
	    JavaPairRDD<Node, Integer> userOutDegToArticle = userToArticleBetter
	    		.aggregateByKey(0, (val, row) -> val + 1, (val1, val2) -> val1 + val2);
	    
	    JavaPairRDD<Node, Integer> articleOutDegToUser = articleToUserBetter
	    		.aggregateByKey(0, (val, row) -> val + 1, (val1, val2) -> val1 + val2);
	    
	    // 4) Join, on from_node, the out-degree PairRDD's with their respective "better" PairRRD's.
	    JavaPairRDD<Node, Tuple2<Tuple2<Node, Double>, Integer>> userToUserJoin = userToUserBetter.join(userOutDegToUser);
	    JavaPairRDD<Node, Tuple2<Tuple2<Node, Double>, Integer>> userToCategoryJoin = userToCategoryBetter.join(userOutDegToCategory);
	    JavaPairRDD<Node, Tuple2<Tuple2<Node, Double>, Integer>> categoryToUserJoin = categoryToUserBetter.join(categoryOutDegToUser);
	    JavaPairRDD<Node, Tuple2<Tuple2<Node, Double>, Integer>> userToArticleJoin = userToArticleBetter.join(userOutDegToArticle);
	    JavaPairRDD<Node, Tuple2<Tuple2<Node, Double>, Integer>> articleToUserJoin = articleToUserBetter.join(articleOutDegToUser);
		
	    // 5a) Scale each <<user, user>, weight>'s edge weight by (0.3 / userOutDegToUser). 
	    JavaPairRDD<Node, Tuple2<Node, Double>> userToUserProperEdges = userToUserJoin.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1, new Tuple2<Node, Double>(entry._2._1._1, entry._2._1._2 * (0.3 / entry._2._2))));
	    
	    // 5b) Scale each <<user, category>, weight>'s edge weight by (0.3 / userOutDegToCategory). 
	    JavaPairRDD<Node, Tuple2<Node, Double>> userToCategoryProperEdges = userToCategoryJoin.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1, new Tuple2<Node, Double>(entry._2._1._1, entry._2._1._2 * (0.3 / entry._2._2))));
	    
	    // 5c) Scale each <<category, user>, weight>'s edge weight by (0.5 / categoryOutDegToUser). 
	    JavaPairRDD<Node, Tuple2<Node, Double>> categoryToUserProperEdges = categoryToUserJoin.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1, new Tuple2<Node, Double>(entry._2._1._1, entry._2._1._2 * (0.5 / entry._2._2))));
	    
	    // 5d) Scale each <<user, article>, weight>'s edge weight by (0.4 / userOutDegToArticle). 
	    JavaPairRDD<Node, Tuple2<Node, Double>> userToArticleProperEdges = userToArticleJoin.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1, new Tuple2<Node, Double>(entry._2._1._1, entry._2._1._2 * (0.4 / entry._2._2))));
	    
	    // 5e) Scale each <<article, user>, weight>'s edge weight by (0.5 / articleOutDegToUser). 
	    JavaPairRDD<Node, Tuple2<Node, Double>> articleToUserProperEdges = articleToUserJoin.mapToPair
	    		(entry -> new Tuple2<Node, Tuple2<Node, Double>>
	    		(entry._1, new Tuple2<Node, Double>(entry._2._1._1, entry._2._1._2 * (0.5 / entry._2._2))));
	    
	    // 6) Convert the PairRDD's back to the original format of <<from_node, to_node>, weight>.
	    JavaPairRDD<Tuple2<Node, Node>, Double> finalUserToUser = userToUserProperEdges.mapToPair
	    		(entry -> new Tuple2<Tuple2<Node, Node>, Double>
	    		(new Tuple2<Node, Node>(entry._1, entry._2._1), entry._2._2));
	    
	    JavaPairRDD<Tuple2<Node, Node>, Double> finalUserToCategory = userToCategoryProperEdges.mapToPair
	    		(entry -> new Tuple2<Tuple2<Node, Node>, Double>
	    		(new Tuple2<Node, Node>(entry._1, entry._2._1), entry._2._2));
	    
	    JavaPairRDD<Tuple2<Node, Node>, Double> finalCategoryToUser = categoryToUserProperEdges.mapToPair
	    		(entry -> new Tuple2<Tuple2<Node, Node>, Double>
	    		(new Tuple2<Node, Node>(entry._1, entry._2._1), entry._2._2));
	    
	    JavaPairRDD<Tuple2<Node, Node>, Double> finalUserToArticle = userToArticleProperEdges.mapToPair
	    		(entry -> new Tuple2<Tuple2<Node, Node>, Double>
	    		(new Tuple2<Node, Node>(entry._1, entry._2._1), entry._2._2));
	    
	    JavaPairRDD<Tuple2<Node, Node>, Double> finalArticleToUser = articleToUserProperEdges.mapToPair
	    		(entry -> new Tuple2<Tuple2<Node, Node>, Double>
	    		(new Tuple2<Node, Node>(entry._1, entry._2._1), entry._2._2));
	    
	    // 7) Union the PairRDD's to create the overallNetwork PairRDD to be output.
	    JavaPairRDD<Tuple2<Node, Node>, Double> overallNetwork = articleCategoryFiltered
	    		.union(finalUserToUser)
	    		.union(finalUserToCategory)
	    		.union(finalCategoryToUser)
	    		.union(finalUserToArticle)
	    		.union(finalArticleToUser);
	    
	    
		JavaRDD<Node> fromNodes = overallNetwork.map(entry -> entry._1._1);
		JavaRDD<Node> toNodes = overallNetwork.map(entry -> entry._1._2);
		long numNodes = fromNodes.union(toNodes).distinct().count();
		
        long numEdges = overallNetwork.count();
        
		System.out.println("The overallNetwork contains " + String.valueOf(numNodes) + " nodes and "
				+ String.valueOf(numEdges) + " edges.");
	    
	    
	    logger.info("getNewsFeedNetwork complete.");
	    
		return overallNetwork;
	}

	/**
	 * Main functionality in the program: read and process the network
	 * 
	 * @param dMax If the maximum change in the user score, considering any 
	 *             LabelMap, falls at or below this value, the algorithm will terminate. 
	 *             
	 * @param iMax The maximum number of absorption iterations allowed to run before
	 *             the algorithm terminates. ***Should use 15.***
	 *             
	 * @param debug If true, outputs the inputUser score of each node after each iteration. 
	 * 
	 * @param inputUser The user score of particular interest for the algorithm (i.e., the user 
	 *             whose news feed will be updated after this algorithm ceases). 
	 *             
	 * @param date The date of the articles that will be considered when choosing an
	 *             article for the user. (Provided in "YYYY-MM-DD" format.)
	 *             
	 * @return The recommended article Node's link, for the <inputUser>.
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public String run(double dMax, int iMax, boolean debug, String inputUser, String date) throws IOException, InterruptedException {
		logger.info("Beginning run method...");

		// Load the network.
		JavaPairRDD<Tuple2<Node, Node>, Double> network = getNewsFeedNetwork(date);
		
		logger.info("Beginning adsorption algorithm on overall network...");
		
		/*
		 * Conduct the adsorption algorithm on the network.
		 */
		
		// A JavaRDD of the nodes of the graph is all that is needed to store each 
		// node's user scores (which are stored as a LabelMap field within each Node object).
		JavaRDD<Node> fromNodes = network.map(pair -> pair._1()._1()).distinct();
		JavaRDD<Node> toNodes = network.map(pair -> pair._1()._2()).distinct();
		JavaRDD<Node> nodes = fromNodes.union(toNodes).distinct();
		
		System.out.println("nodes size is: " + String.valueOf(nodes.count()));
		
        // Determine the source nodes. Note that their user scores will stay constant 
		// throughout the algorithm (in the case of a non-user node, 0.0 for all 
		// user scores), thus their user scores may be freely ignored. 
		JavaRDD<Node> sourceNodes = nodes.subtract(toNodes);
		
		System.out.println("sourceNodes size is: " + String.valueOf(sourceNodes.count()));
		
		// The non-source nodes will be the ones with user scores we care about.
		// This is the variable we will continue to update with the most current
		// postIterLabelMaps for each of its nodes. 
		JavaRDD<Node> nonSourceNodes = nodes.subtract(sourceNodes);

		System.out.println("nonSourceNodes size is: " + String.valueOf(nonSourceNodes.count()));
		
		System.out.println("diff in nodasdf size is: " + String.valueOf(nonSourceNodes.subtract(sourceNodes).count()));
		
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
				
				/*
				 * Map<String, Double> edgeScaledMap = fromNode.edgeScaleMap(edgeWeight);
				 */
				
				Map<String, Double> edgeScaledMap = new HashMap<>();
				
				Iterator<Entry<String, Double>> iter = fromNode.postIterLabelMap.entrySet().iterator();
				while (iter.hasNext()) {
					Entry<String, Double> entryy = iter.next();
					String user = entryy.getKey();
					Double entryScore = entryy.getValue();
					edgeScaledMap.put(user, entryScore * edgeWeight);
				}
				
				/*
				 * toNode.addLabelMap(edgeScaledMap);
				 */
				
				Iterator<Entry<String, Double>> inputIter = edgeScaledMap.entrySet().iterator();
				while (inputIter.hasNext()) {
					Entry<String, Double> inputEntry = inputIter.next();
					String user = inputEntry.getKey();
					Double inputScore = inputEntry.getValue();
					
					Double currScore = toNode.midIterLabelMap.get(user);
					if (currScore != null) {
						toNode.midIterLabelMap.put(user, currScore + inputScore);		
					} else {
						// toNode had not heard of the user until this point!
						// Update the midIterLabelMap to include an entry <user, inputScore>.
						// (I.e., currScore was effectively 0.0 beforehand.)
						toNode.midIterLabelMap.put(user, inputScore);	
					}
				}
				
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
							(val, row) -> 
					
					        /*
					         * (new coalesceSeqOrCombFunc()).call(val1, val2);
					         */
					     
							{if (val.isEmpty()) {
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
							}},      
							(val1, val2) -> 
							
							/*
					         * (new coalesceSeqOrCombFunc()).call(val1, val2);
					         */
					     
							{if (val1.isEmpty()) {
								// The empty map serves as our zeroValue in the aggregateByKey() call.
								// It is to be interpreted as a map from each user to the double 0.0.
								return val2;
							} else {
								// A non-trivial sumMap:
								HashMap<String, Double> sumMap = new HashMap<>();
								
								Iterator<Entry<String, Double>> iter = val2.entrySet().iterator();
								while (iter.hasNext()) {
									Entry<String, Double> entry = iter.next();
									String user = entry.getKey();
									Double score = entry.getValue();
									
									Double valMapScore = val1.get(user);
						            if (valMapScore != null) {
						            	sumMap.put(user, score + valMapScore);
						            } else {
						            	// The valMapScore may be interpreted as 0.0.
						            	// (An absence of a <User, Score> entry in any LabelMap is
						            	// equivalent to a mapping of <User, 0.0>.)
						            	sumMap.put(user, score);
						            }	
								}
								
								// We must now consider the user entries that appear in the val1Map
								// but do NOT appear in the val2Map.
								
								iter = val1.entrySet().iterator();
								while (iter.hasNext()) {
									Entry<String, Double> entry = iter.next();
									String user = entry.getKey();
									Double score = entry.getValue();
									
									Double rowMapScore = val2.get(user);
						            if (rowMapScore == null) {
						            	sumMap.put(user, score);
						            } 
								}
								
								return sumMap;				
							}});
			
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
				
				/*
				 * node.finalizeLabelMap();
				 */
				
				// 1) normalizeLabelMap();
				// Sum the scores across all users.
				Double scoreSum = 0.0;
				
				Iterator<Entry<String, Double>> firstIter = node.midIterLabelMap.entrySet().iterator();
				while (firstIter.hasNext()) {
					Entry<String, Double> entry = firstIter.next();
					Double entryScore = entry.getValue();
					scoreSum += entryScore;
				}
				
				// Scale each score by (1 / scoreSum).
				Iterator<Entry<String, Double>> secondIter = node.midIterLabelMap.entrySet().iterator();
				while (secondIter.hasNext()) {
					Entry<String, Double> entry = secondIter.next();
					String user = entry.getKey();
					Double entryScore = entry.getValue();
					node.midIterLabelMap.put(user, entryScore / scoreSum);
				}
				
				// 2) Set postIterLabelMap -> midIterLabelMap.
				Iterator<Entry<String, Double>> iter = node.midIterLabelMap.entrySet().iterator();
				while (iter.hasNext()) {
					Entry<String, Double> entry = iter.next();
					String user = entry.getKey();
					Double entryScore = entry.getValue();
					node.postIterLabelMap.put(user, entryScore);
				}
				
				// 3) zeroLabelMap();
				Iterator<Entry<String, Double>> iterr = node.midIterLabelMap.entrySet().iterator();
				while (iterr.hasNext()) {
					Entry<String, Double> entryy = iterr.next();
					String user = entryy.getKey();
					node.midIterLabelMap.put(user, 0.0);
				}
			});
			
			/*
			 * ?? Have a debug mode with printed information every iteration ?? (Optional)
			 */
			
			if (debug) {
			
			}
			
			System.out.println("nonSourceNodes size is: " + String.valueOf(nonSourceNodes.count()));
			System.out.println("updatedNonSourceNodes size is: " + String.valueOf(updatedNonSourceNodes.count()));
			
			
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
			
			System.out.println("newNodeAndOldNode size is: " + String.valueOf(newNodeAndOldNode.count()));
			
			// Some article nodes may have a null mapping for the inputUser. 
			// (They were never "told" about them.)
			// This should be equivalent to a mapping to the value 0.0.
			
			newNodeAndOldNode.foreach(entry -> 
					{Node future = entry._1;
					 Node past = entry._2;
					 
					 future.postIterLabelMap.putIfAbsent(inputUser, 0.0);
					 past.postIterLabelMap.putIfAbsent(inputUser, 0.0);		
					});
			
			JavaPairRDD<Node, Double> nodeAndScoreDiff = newNodeAndOldNode
					.mapToPair(pair -> new Tuple2<Node, Double>(pair._1,
							pair._1.postIterLabelMap.get(inputUser)
							- pair._2.postIterLabelMap.get(inputUser)));
			
			System.out.println("nodeAndScoreDiff size is: " + String.valueOf(nodeAndScoreDiff.count()));

			List<Tuple2<Double, Node>> maxScoreDiffList = nodeAndScoreDiff
					.mapToPair(pair -> new Tuple2<Double, Node>(pair._2, pair._1))
					.sortByKey(false)
					.take(1);
			System.out.println("The size of maxScoreDiffList is: " + String.valueOf(maxScoreDiffList.size()));
			
			// double maxScoreDiff = maxScoreDiffList.get(0)._1();
			double maxScoreDiff = 19.0;
			
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
				
				/*
				 * toNode.zeroLabelMap();	
				 */
				
				Iterator<Entry<String, Double>> iter = toNode.midIterLabelMap.entrySet().iterator();
				while (iter.hasNext()) {
					Entry<String, Double> entryy = iter.next();
					String user = entryy.getKey();
					toNode.midIterLabelMap.put(user, 0.0);
				}
			});
				
			// We can now pass nonSourceNodes to its updated value. 
			// (To refer back to it in the upcoming iteration, when testing for convergence.)
			nonSourceNodes = updatedNonSourceNodes;
			
			iterationCount += 1;
		}
		
		/*
		 * ADSORPTION ALGORITHM CONCLUSION:
		 * Take the resulting updatedNonSourceNodes and compile the nodes that are type = "article" 
		 * with publishDate = <date>, disregarding the articles that were already recommended to the 
		 * <inputUser>. Among the remaining articles, randomly select one, using probabilities equal 
		 * to their scaled <inputUser> scores from each node's postIterLabelMap.
		 */

		JavaRDD<Node> todaysArticles = updatedNonSourceNodes
				.filter(node -> node.type.equals("article") && node.publishDate.equals(date));
		
		System.out.println("todaysArticles size is: " + String.valueOf(todaysArticles.count()));
		
		// *Filter out the articles that were already recommended to the <inputUser>.*
		// 1) Accumulate a List<Node> of the query results. 
		ItemCollection<QueryOutcome> queryResults = recommended.query("UserID", inputUser);
		List<Node> queryResultNodes = new ArrayList<>(); // Will become a JavaRDD<Node>.
		
		for (Item item : queryResults) {
			String articleLink = item.getString("link");
			Node articleNode = new Node(articleLink, "article");
			queryResultNodes.add(articleNode);
		}
		
		// 2) context.parallelize() the List<Node> into a JavaRDD<Node> and invoke subtract().
		JavaRDD<Node> queryResultRDD = context.parallelize(queryResultNodes);
		JavaRDD<Node> validArticles = todaysArticles.subtract(queryResultRDD);
		
		System.out.println("validArticles size is: " + String.valueOf(validArticles.count()));
		
		validArticles.foreach(node -> System.out.println("Here is an inputUser score: " + String.valueOf(node.postIterLabelMap.get(inputUser))));
		
		// *Extract the <inputUser> score from each article's postIterLabelMap.*
		
		// 1) Some article nodes may have a null mapping for the inputUser. 
		//    (They were never "told" about them.)
		//    This should be equivalent to a mapping to the value 0.0.
		validArticles.foreach(node -> node.postIterLabelMap.putIfAbsent(inputUser, 0.0));
		
		validArticles.foreach(node -> System.out.println("Here is an inputUser score: " + String.valueOf(node.postIterLabelMap.get(inputUser))));
		
		// 2) Now proceed as intended.
		JavaPairRDD<Node, Double> articleAndUserScore = validArticles
				.mapToPair(node -> new Tuple2<Node, Double>(node, node.postIterLabelMap.get(inputUser)));
		
		// *Scale the <inputUser> scores such that they sum to 1.*
		
		// 1) First obtain the sum of all <inputUser> scores across the nodes.
		JavaRDD<Double> userScores = articleAndUserScore.map(pair -> pair._2);
		
		Double scoreSum = userScores.aggregate(0.0, (val, row) -> val + row, (val1, val2) -> val1 + val2);
		
		// 2) Next scale each <inputUser> score by (1 / scoreSum).
		JavaPairRDD<Node, Double> articleAndScaledScore = articleAndUserScore
				.mapToPair(entry -> new Tuple2<Node, Double>(entry._1, entry._2 / scoreSum));
		
		// *Randomly select and output an article using the scaled <inputUser> scores as weights.*
		
		// 1) Represent the PairRDD<Node, Double> as a list of Pair<Node, Double> objects.
		// (Required by the eventually-invoked EnumeratedDistribution object.)
		JavaRDD<Pair<Node, Double>> asPairs = articleAndScaledScore
				.map(entry -> new Pair<>(entry._1, entry._2));
		
		List<Pair<Node, Double>> asPairList = asPairs.collect();
		
		System.out.println("asPairList size is: " + String.valueOf(asPairList.size()));
		//return "boy";
		
		///*
		// 2) Complete the weighted selection using an EnumeratedDistribution object.
		Node selectedNode = new EnumeratedDistribution<Node>(asPairList).sample();
		
		String selectedLink = null;
		
		if (selectedNode != null) {
			// Nodes with type = "article" have links as their name field.
			selectedLink = selectedNode.name;
			
			// Upload the recommendation information to the Recommended Table.
			// (Future invocations of the adsorption algorithm on this <inputUser> will
			// now avoid re-recommending this article.) 
			
			Item item = new Item();
			item.withPrimaryKey("UserID", inputUser, "link", selectedLink);
			item.withString("date", date);
			
			recommended.putItem(item);	
		}
			
		logger.info("*** Finished adsorption algorithm! ***");
		
		System.out.println("The selected link is: " + selectedLink);
		return selectedLink;
		//*/
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
	
	/*
	
	// Private class whose call() method is invoked by a filter() in getNewsFeedNetwork() 
	// when filtering out article nodes (and their corresponding edges) with publish dates
	// after an input <date>.
	private class FilterOutAfterDate implements Function<Tuple2<Tuple2<Node, Node>, Double>, Boolean>, Serializable {
		
		private static final long serialVersionUID = 1L;
		
		LocalDate currDateObj; // A LocalDate object created from the input <date> in the class constructor.
		
		// <date> provided in "YYYY-MM-DD" format. We will filter out the articles published after this date.
		FilterOutAfterDate(String date) {
			currDateObj = LocalDate.parse(date);
		}
		
		public Boolean call(Tuple2<Tuple2<Node, Node>, Double> entry) {
			Node fromNode = entry._1._1;
			Node toNode = entry._1._2;
			
			// Test the fromNode.
			if (fromNode.type.equals("article")) {
				String publishDate = fromNode.publishDate;
				
				// We add four years to the publish date of an article when interpreting it
				// (per the instructions). 
				LocalDate nodeDateObj = LocalDate.parse(publishDate).plusYears(4);
				
				if (nodeDateObj.isAfter(currDateObj)) {
					return false;
				}
				
			}
			
			// Test the toNode.
			if (toNode.type.equals("article")) {
				String publishDate = toNode.publishDate;
				
				// We add four years to the publish date of an article when interpreting it
				// (per the instructions). 
				LocalDate nodeDateObj = LocalDate.parse(publishDate).plusYears(4);
				
				if (nodeDateObj.isAfter(currDateObj)) {
					return false;
				}
			}
			
			// All clear!
			return true;
		}
	}

    */
	
	/*

	// Private class whose call() method is invoked in the aggregateByKey() method 
	// that coalesces the nodes in updatedNonSourceNodes. Serves as the seq or comb function.
	// (See JavaDocs for clarification.)
	private class coalesceSeqOrCombFunc implements Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>>, Serializable {
		
		private static final long serialVersionUID = 1L;

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
	
	*/
	
	/**
	 * A helper class to define each node type (user, category, article) all under a single object.
	 * 
	 * @author nets212
	 */
	private static class Node implements Serializable {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
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
		
		/*
		 
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
		
		*/
		
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
	
	public static void main(String[] args) {
		ArticleAdsorption aa = new ArticleAdsorption();

		try {
			aa.initialize();
			aa.run(0.5, 1, false, "scott", "2022-05-24");
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			aa.shutdown();
		}
	}
	
}
