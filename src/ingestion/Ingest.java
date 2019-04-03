package ingestion;

/**
 * Responsible for the ingestion of data into a derby or mongo database.
 */
public class Ingest {

    /**
     * Runs the database CSV ingestion script
     *
     * @param args [mongo/derby] [number of records] [offset]
     */
    public static void main(String[] args) {

        // Check if the correct amount of arguments have been passed in.
        if (args.length != 1) {
            System.out.println("Not enough arguments. ingest [mongo/derby]");
            return;
        }

        try {

            // Run the ingest for derby
            if (args[0].equalsIgnoreCase(ConfigProvider.DERBY)) {
                DerbyDB derby = new DerbyDB();
                derby.initialiseTables();
                derby.ingest();
            }

            // Run the ingest for mongo
            if (args[0].equalsIgnoreCase(ConfigProvider.MONGO)) {
                MongoDB mongo = new MongoDB();
                mongo.ingest();
            }
        }
        catch (DatabaseException e) {
            System.out.println("ERROR: " + e);
        }
    }
}