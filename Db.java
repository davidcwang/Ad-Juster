/**
 * The Db class provides an interface to a mongoDB database.
 *
 * Filename: Db.java
 * @author: David Wang
 *
 */

import com.mongodb.AggregationOutput;
import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Db {

	private static final String _HOST_NAME = "localhost";
	private static final int _PORT = 27017;
	private static final String _DATABASE_NAME = "client";
	public static final String CAMPAIGNS_COLLECTION = "campaigns";
	public static final String CREATIVES_COLLECTION = "creatives";

    /**
     * Use the MongoDB aggregate command to join the creatives with parentIds
     * that equal to the id of the campaign field. Once the fields are joined
     * we group the entries with the same id field in order to sum up the clicks
     * and views.
     * The aggregate function takes in a list of different aggregation
     * functions which can be specified. There are 4 aggregation functions in
     * our case.
     *
     * 1. Performs the equivalent of a left inner join of creatives and
     *    campaigns on creatives.parendId=campaigns.id.
     * 2. Merges the fields of the two collections.
     * 3. Causes the campaigns object to not be displayed.
     * 4. Groups the entries by parentId and sums the clicks and views field
     *    and adds totalClicks and totalViews fields.
     *
     * The query that will be constructed is shown below.

     /////////////////// BEGIN QUERY ///////////////////
		db.creatives.aggregate([
			{
				$lookup: {
					from: "campaigns",
					localField: "parentId",
					foreignField: "id",
					as: "fromCampaigns"
				}
			},
			{
				$replaceRoot: {newRoot: {$mergeObjects: [{$arrayElemAt: ["$fromCampaigns", 0]}, "$$ROOT"]}}
			},
			{$project: {fromCampaigns: 0}},
			{
				$group:
				{
					_id: "$parentId",
					cpm: {$first: "cpm"},
					name: {$first: "$name"},
					startDate: {$first: "$startDate"},
					totalClicks: {$sum: "$$clicks"},
					totalViews: {$sum: "$views"}
				}
			}
		])
     /////////////////// END QUERY ///////////////////
     *
     * @exception Exception Thrown if an error occurs when connecting with the db.
     * @return Cursor A cursor to the first DBObject.
     */
    public static Cursor getTotalClicksAndViewsFromCampaigns() throws Exception {
        DB db = _connect();
        DBCollection coll = db.getCollection(CREATIVES_COLLECTION);
        System.out.println("Performing aggregation query of" 
              +  " campaigns and creatives...");
        List<DBObject> pipeline = new ArrayList<DBObject>();
        AggregationOptions options = AggregationOptions
                                    .builder()
                                    .outputMode(AggregationOptions
                                                .OutputMode
                                                .CURSOR)
                                                .build();

        // 1 $lookup
        DBObject lookup = new BasicDBObject();
        DBObject lookupOptions = new BasicDBObject();
        lookupOptions.put("from", "campaigns");
        lookupOptions.put("localField", "parentId");
        lookupOptions.put("foreignField", "id");
        lookupOptions.put("as", "fromCampaigns");
        lookup.put("$lookup", lookupOptions);


        // 2 $replaceRoot
        // We build complete $replaceRoot Aggregation option step-by-step.
        // Each step will use the map or array created from the previous step.
        // $replaceRoot: {newRoot: {$mergeObjects: [{$arrayElemAt: ["$fromCampaigns", 0]}, "$$ROOT"]}}

        // {$arrayElemAt: ["$fromCampaigns", 0]}
        DBObject arrayElemAtMap = new BasicDBObject();
        arrayElemAtMap.put("$arrayElemAt", Arrays.asList("$fromCampaigns", 0));

        // {$mergeObjects: [{$arrayElemAt: ["$fromCampaigns", 0]}, "$$ROOT"]}}
        DBObject mergeObjects = new BasicDBObject();
        mergeObjects.put("$mergeObjects", 
                         Arrays.asList(arrayElemAtMap, "$$ROOT"));

        // {newRoot: {$mergeObjects: [{$arrayElemAt: ["$fromCampaigns", 0]}, "$$ROOT"]}}
        DBObject replaceRootMap = new BasicDBObject();
        replaceRootMap.put("newRoot", mergeObjects);

        // $replaceRoot: {newRoot: {$mergeObjects: [{$arrayElemAt: ["$fromCampaigns", 0]}, "$$ROOT"]}}
        DBObject replaceRoot = new BasicDBObject();
        replaceRoot.put("$replaceRoot", replaceRootMap);


        // 3  $project
        // {$project: {fromCampaigns: 0}},
        DBObject project = new BasicDBObject();
        DBObject projectMap = new BasicDBObject();
        projectMap.put("fromCampaigns", 0);
        project.put("$project", projectMap);

        // 4 $group
        DBObject group = new BasicDBObject();
        DBObject groupOptions = new BasicDBObject();
        groupOptions.put("_id", "$parentId");
        groupOptions.put("cpm", new BasicDBObject("$first", "$cpm"));
        groupOptions.put("name", new BasicDBObject("$first", "$name"));
        groupOptions.put("startDate", new BasicDBObject("$first", "$startDate"));
        groupOptions.put("totalClicks", new BasicDBObject("$sum", "$clicks"));
        groupOptions.put("totalViews", new BasicDBObject("$sum", "$views"));
        group.put("$group", groupOptions);

        // Create pipeline.
        pipeline.add(lookup);
        pipeline.add(replaceRoot);
        pipeline.add(project);
        pipeline.add(group);

        Cursor cursor = coll.aggregate(pipeline, options);

        System.out.println("Aggregation Query Complete");

        return cursor;
    }

    /**
     * Iterates over each element from the cursor and prints them to 
     * standard out.
     *
     * @param cursor The cursor pointing to a DBObject.
     * @exception Exception Exception is thrown if error connection to
     * database.
     * @return None
     */
    public static void printFromCursor(Cursor cursor) {
        while (cursor.hasNext()) {
            DBObject obj = cursor.next();
            System.out.println(obj);
        }
    }

    /**
     * Drops the database specified by _DATABASE_NAME.
     *
     * @exception Exception If any erros with connecting or dropping the database.
     */
    public static void dropDatabase() throws Exception {
        System.out.println("Dropping database: " + _DATABASE_NAME + "....");
        DB db = _connect();
        db.dropDatabase();
        System.out.println("Dropped database: " + _DATABASE_NAME);
    }

    /**
     * Inserts data from a JSONArray to the specified collection.
     *
     * @param data An array of json objects.
     * @param collectionName The name of the mongoDB collection.
     * @exception Exception Exception is thrown if error connection to
     * database.
     * @return None
     */
    public static void insertObjects(JSONArray data, String collectionName) throws Exception {
        DB db = _connect();
        DBCollection coll = db.getCollection(collectionName);
        System.out.println("Inserting " + data.size() + " objects...");

        List<DBObject> dbObjectList = _JSONArrayToDBObject(data);
        WriteResult writeResult = coll.insert(dbObjectList);
        System.out.println("Insertion complete");
        _disconnect();
    }

    /**
     * Returns the number of entries in that collection. This implementation
     * assumes that we won't have more than MAX_LONG entries, otherwise, we
     * will need to use BigInteger.
     *
     * @param collectionName The name of the mongoDB collection.
     * @exception Exception Exception is thrown if error connection to
     * database.
     * @return None
     */
    public static long getCollectionCount(String collectionName) throws Exception {
        DB db = _connect();
        DBCollection coll = db.getCollection(collectionName);
        return coll.count();
    }


    /**
     * Converts a list of JSON objects to a mongo DBObject.
     *
     * @param array An array of JSON objects.
     * @return None
     */
	private static List<DBObject> _JSONArrayToDBObject(JSONArray array) {
		List<DBObject> list = new ArrayList<DBObject>();
        System.out.println("Entering " + array.size() + " objects....");

		for (int i = 0; i < array.size(); i++) {
			String object = array.get(i).toString();
			DBObject dbObject = (DBObject) JSON.parse(object);
			list.add(dbObject);
		}

        System.out.println("Finished Inserting objects");
        return list;
	}

    /**
     * Connects to a mongoDB database with the specified hostname, port
     * and database name.
     *
     * @exception Exception Exception is thrown if error connection to
     * database.
     * @return None
     */
	private static DB _connect() throws Exception {
        System.out.println("Connecting to " + _DATABASE_NAME + "...");
        MongoClient mongoClient = new MongoClient(_HOST_NAME , _PORT);
        DB db = mongoClient.getDB(_DATABASE_NAME);
        System.out.println("Connected to " + _DATABASE_NAME);
		return db;
	}

    /**
     * Disconnects to a mongoDB database with the specified hostname, port
     * and database name.
     *
     * @exception Exception Exception is thrown if error disconnecting to
     * database.
     * @return None
     */
	private static DB _disconnect() throws Exception {
		MongoClient mongoClient = new MongoClient(_HOST_NAME , _PORT);
        DB db = mongoClient.getDB(_DATABASE_NAME);
        System.out.println("Disconnected from " + _DATABASE_NAME);
        return db;
	}
}
