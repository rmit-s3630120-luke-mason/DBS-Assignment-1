package ingestion;

import java.io.*;
import java.util.Properties;

import static ingestion.ConfigProvider.getConfigProvider;

/**
 * Responsible for the ingestion of data into a derby or mongo database.
 */
public class Ingest {
    private static final String DERBY = "derby";
    private static final String MONGO = "mongo";

    /**
     * Runs the database CSV ingestion script
     *
     * @param args [mongo/derby] [number of records] [offset]
     */
    public static void main(String[] args) {

        // Check if the correct amount of arguments have been passed in.
        if (args.length != 3) {
            System.out.println("Not enough arguments. ingest [mongo/derby] [number of records] [offset]");
            return;
        }

        int records;
        int offset;
        try {

            // Convert args to an Integer value
            records = Integer.parseInt(args[1]);
            offset = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("The given record or offset value is an invalid number");
            return;
        }

        // Check if records is less than 1
        if (records < 1) {
            System.out.println("Error: Records must be a minimum of 1");
            return;
        }

        // Check if offset is negative (invalid)
        if (offset < 0) {
            System.out.println("Error: Offset cannot be negative.");
            return;
        }

        try {

            // Run the ingest for derby
            if (args[0].equalsIgnoreCase(DERBY)) {
                DerbyDB derby = createDerbyDB();
                derby.initialiseTables();
                derby.ingest();
            }

            // Run the ingest for mongo
            if (args[0].equalsIgnoreCase(MONGO)) {
                MongoDB mongo = new MongoDB();
                Properties properties = getConfigProvider().getPropertyFile("mongoConfig");
                String destination = properties.getProperty("jsonFileDestination");
                String filename = properties.getProperty("jsonFilename");
                mongo.createJSONFromCSVs(destination + filename);
                extractDataAndIngest(mongo, file, records, offset);
                mongo.exit();
            }
        }
        catch (DatabaseException e) {
            System.out.println("ERROR: " + e);
        }
    }

    /**
     * Creates a derbyDB based off the properties in the properties configuration file.
     *
     * @return The DerbyDB instance.
     * @throws DatabaseException
     */
    private static DerbyDB createDerbyDB() throws DatabaseException {
        Properties properties;

        // load a properties file
        try (InputStream input = new FileInputStream("../config/derbyConfig.properties")) {
            properties = new Properties();
            properties.load(input);
        } catch (IOException e) {
            throw new DatabaseException("Could not load in Derby properties - " + e);
        }

        // Get jdbc driver
        String driver = properties.getProperty("driver");
        String url = properties.getProperty("url");

        return new DerbyDB(url, driver);
    }
}