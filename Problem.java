/**
 * Implementation for each of the problems provide in the homework writeup.
 *
 * Filename: Problem.java
 * @author: David Wang
 *
 */

import com.mongodb.Cursor;
import org.json.simple.JSONArray;

public class Problem {

    /**
     * Uses Client to pull the data from the API,
     * and save it locally to a mongoDB database.
     *
     * @exception Exception thrown if passed up from any of the functions.
     */

    public static void problem1() throws Exception {
        // Drop the database in order to make sure there's no existing
        // campaigns or creatives for calculation purposes.
        Db.dropDatabase();

        JSONArray creativesArray = Client.getCreatives();
        System.out.println("Number of creatives before inserting objects: " + 
                            Db.getCollectionCount(Db.CREATIVES_COLLECTION));
        Db.insertObjects(creativesArray, Db.CREATIVES_COLLECTION);
        System.out.println("Number of creatives after inserting objects: " + 
                            Db.getCollectionCount(Db.CREATIVES_COLLECTION));

        JSONArray campaignsArray = Client.getCampaigns();
        System.out.println("Number of campaigns before inserting objects: " + 
                            Db.getCollectionCount(Db.CAMPAIGNS_COLLECTION));
        Db.insertObjects(campaignsArray, Db.CAMPAIGNS_COLLECTION);
        System.out.println("Number of campaigns after inserting objects: " + 
                            Db.getCollectionCount(Db.CAMPAIGNS_COLLECTION));
    }

    /**
     * Write a database command/query (using your database of choice) to
     * calculate total clicks and views at the campaign level per child
     * creatives.
     *
     * @exception Exception thrown if passed up from any of the functions.
     */
    public static void problem2() throws Exception {
        Cursor cursor = Db.getTotalClicksAndViewsFromCampaigns();
        Db.printFromCursor(cursor);
    }

    /**
     * Outpus the results from Problem 2 in a CSV file.
     *
     * @exception Exception thrown if passed up from any of the functions.
     */
    public static void problem3() throws Exception {
        Cursor cursor = Db.getTotalClicksAndViewsFromCampaigns();
        Client.outputCampaignsToCSV(cursor, "Campaign_Clicks_and_Views.csv");
    }

    /**
     * Revenue is calculated using CPM and views. Using the formula
     * Revenue = CPM * views / 1000 please include revenue data at the
     * campaign level in the CSV file.
     *
     * @exception Exception thrown if passed up from any of the functions.
     */
    public static void extraCredit() throws Exception {
        Cursor cursor = Db.getTotalClicksAndViewsFromCampaigns();
        Client.outputRevenuesCampaignsToCSV(cursor, "Campaign_with_Revenue.csv");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("################   Problem 1  #################");
        Problem.problem1();
        System.out.println("\n\n");

        System.out.println("################   Problem 2  #################");
        Problem.problem2();
        System.out.println("\n\n");

        System.out.println("################   Problem 3  #################");
        Problem.problem3();
        System.out.println("\n\n");

        System.out.println("################ Extra Credit #################");
        Problem.extraCredit();
        System.out.println("\n\n");
    }
}
