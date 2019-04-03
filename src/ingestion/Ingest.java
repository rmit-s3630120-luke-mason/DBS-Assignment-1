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
            if (args[0].equalsIgnoreCase(ConfigProvider.DERBY)) {
                DerbyDB derby = new DerbyDB();
                derby.initialiseTables();
                derby.ingest();
            }

            // Run the ingest for mongo
            if (args[0].equalsIgnoreCase(ConfigProvider.MONGO)) {
                MongoDB mongo = new MongoDB();
                mongo.createJSONFromCSVs();
                mongo.ingest();
            }
        }
        catch (DatabaseException e) {
            System.out.println("ERROR: " + e);
        }
    }
}