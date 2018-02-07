/**
 * Filename: Client.java
 * @author: David Wang
 *
 * A client object that handles the HTTP requests to get the Campaigns and
 * Creatives data. Also provides methods for outputing data to a CSV file.
 *
 * http://homework.ad-juster.com/api/campaigns GET
 * http://homework.ad-juster.com/api/creatives GET
 */

import com.mongodb.Cursor;
import com.mongodb.DBObject;

import java.lang.StringBuffer;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.HttpResponse;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

public class Client {

    private static final String _CAMPAIGNS_ENDPOINT = "http://homework.ad-juster.com/api/campaigns";
    private static final String _CREATIVES_ENDPOINT = "http://homework.ad-juster.com/api/creatives";

    /**
     * Iterates over each Campaign from the cursor object and prints them out
     * in CSV format provided by the filename. The Campaign objects must contain
     * the followign fields: _id, cpm, name, startDate, totalClicks, totalViews.
     * totalClicks and totalViews are calculated on the fly from 
     * Db.getTotalClicksAndViewsFromCampaigns()
     *
     * @param cursor A cursor that points to the first campaign DBObject.
     * @param filename The name of the CSV file to print out the campaigns to.
     * @return None
     * @exception IOException An exception is thrown if a file is not able to be
     * opened or written to.
     */
	public static void outputCampaignsToCSV(Cursor cursor, String filename) throws IOException {
        // Try to create the file if it doesn't exist.
        File file = new File(filename);
        if (!file.exists()) {
            file.createNewFile();
        }

        System.out.println("Writing campaigns to " + filename + "...");
		BufferedWriter writer = Files.newBufferedWriter(Paths.get(filename), 
                                                        Charset.forName("UTF-8"),
                                                        StandardOpenOption.WRITE);

		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
				.withHeader("id", "cpm", "name", "startDate", 
							"totalClicks", "totalViews"));


        while (cursor.hasNext()) {
            DBObject obj = cursor.next();

            Object id = obj.get("_id");
            Object cpm = obj.get("cpm");
            Object name = obj.get("name");
            Object startDate = obj.get("startDate");
            Object totalClicks = obj.get("totalClicks");
            Object totalViews = obj.get("totalViews");

            csvPrinter.printRecord(id, cpm, name, startDate, totalClicks, totalViews);
        }

		csvPrinter.flush();            
        System.out.println("Finished writing to " + filename);
	}

    /**
     * Iterates over each Campaign from the cursor object and prints them out
     * in CSV format provided by the filename. The Campaign objects must contain
     * the followign fields: _id, cpm, name, startDate, totalClicks, totalViews.
     * totalClicks and totalViews are calculated on the fly from 
     * Db.getTotalClicksAndViewsFromCampaigns()
     *
     * This function is similar to outputCampaigns except that a revenue column
     * is added. Note: Revenue is calculated with the following formula:
     * revenue = cpm * totalViews / 1000
     * The cpm value is parsed from a string to a float (e.g. "$46.00" -> 46.00)
     * The conversion skips the first character in the string, because it is
     * assumed that there will always be a "$" at index 0.
     *
     * @param cursor A cursor that points to the first campaign DBObject.
     * @param filename The name of the CSV file to print out the campaigns to.
     * @return None
     * @exception IOException An exception is thrown if a file is not able to be
     * opened or written to.
     */
	public static void outputRevenuesCampaignsToCSV(Cursor cursor, String filename) throws IOException {
        // Try to create the file if it doesn't exist.
        File file = new File(filename);
        if (!file.exists()) {
            file.createNewFile();
        }

        System.out.println("Writing campaigns to " + filename + "...");
		BufferedWriter writer = Files.newBufferedWriter(Paths.get(filename), 
                                                        Charset.forName("UTF-8"),
                                                        StandardOpenOption.WRITE);

		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
				.withHeader("id", "cpm", "name", "startDate", 
							"totalClicks", "totalViews", "revenue"));


        while (cursor.hasNext()) {
            DBObject obj = cursor.next();

            Object id = obj.get("_id");
            Object cpm = obj.get("cpm");
            Object name = obj.get("name");
            Object startDate = obj.get("startDate");
            Object totalClicks = obj.get("totalClicks");
            Object totalViews = obj.get("totalViews");

            // Remove the dollar sign in the cpm and convert to a float.
            String cpmString = cpm.toString();
            float cpmFloat = Float.parseFloat(cpmString.substring(1, cpmString.length()));
            Object revenue = cpmFloat * (int) totalViews / 1000;

            csvPrinter.printRecord(id, cpm, name, startDate, totalClicks, totalViews, revenue);
        }

		csvPrinter.flush();            
        System.out.println("Finished writing to " + filename);
	}

    /**
     *  Gets the Creative JSON objects from the endpoint specified at
     *  _CREATIVES_ENDPOINT and returns them as a JSONArray.
     *
     *  @return JSONArray An array of Creative objects in JSON format.
     */
    public static JSONArray getCreatives() throws Exception {
        return _getJSONArrayFromUrl(_CREATIVES_ENDPOINT);
    }

    /**
     *  Gets the Campaign JSON objects from the endpoint specified at
     *  _CAMPAIGN_ENDPOINT and returns them as a JSONArray.
     *
     *  @return JSONArray An array of Campaign objects in JSON format.
     */
    public static JSONArray getCampaigns() throws Exception {
        return _getJSONArrayFromUrl(_CAMPAIGNS_ENDPOINT);
    }

    /**
     * Gets data in the form of JSON by establishing a HTTP connection from
     * the passed in url. The JSON data must be in the form of an array.
     *
     * @param url The url endpoint to the JSON data.
     * @return JSON Array An array of JSON objects.
     * @exception Exception An exception is thrown if there is an error in the 
     * HTTP request.
     */
    private static JSONArray _getJSONArrayFromUrl(String url) throws Exception {
        System.out.println("Connecting to " + url);
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);

        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));

        // Convert the buffered data into a buffered string
        System.out.println("Converting buffered data into string...");
        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        // Parse the json string into an array
        System.out.println("Parsing json object...");
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(result.toString());
        JSONArray array = (JSONArray) obj;
        System.out.println("Parsing Complete");
        return array;
    }
}
