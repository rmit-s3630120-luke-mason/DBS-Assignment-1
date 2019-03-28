import java.io.*;
import java.sql.*;
import java.util.Properties;

/**
 * Is the class responsible for the ingestion execution
 * and ingestion relation functions
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
                Properties properties;

                // load a properties file
                try (InputStream input = new FileInputStream("../resources/derbyConfig.properties")) {
                    properties = new Properties();
                    properties.load(input);
                } catch (IOException e) {
                    throw new DatabaseException("Could not load in Derby properties - " + e);
                }

//                String driver = "org.apache.derby.jdbc.EmbeddedDriver";
//                try {
//                    Class.forName(driver);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    throw new DatabaseException("Could not create derby driver - " + e);
//                }
                Runtime.getRuntime().exec("java org.apache.derby.tools.ij");

                DerbyDB derby = new DerbyDB("connect '" + properties.getProperty("protocol") +
                        properties.getProperty("dbName") + "';");

                derby.initialiseTables();
                derby.ingestTables();


                derby.exit();
            }

            // Run the ingest for mongo
//            if (args[0].equalsIgnoreCase(MONGO)) {
//                MongoDB mongo = new MongoDB();
//                mongo.connect();
//                extractDataAndIngest(mongo, file, records, offset);
//                mongo.exit();
//            }
        }
//        catch(IOException e) {
//            System.out.println("ERROR: Couldn't read in file - " + e);
//        }
        catch (DatabaseException e) {
            System.out.println("ERROR: " + e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class DerbyDB extends Database {

    private static final int SQL_STATEMENT_COUNT = 3;

    /**
     * Constructor that loads the properties of the derby config.
     */
    public DerbyDB(String url) throws DatabaseException {
        super(url);
    }

    /**
     * Ingests the field values into the database.
     *
     * @param tableName
     * @param filename
     */
    public void ingest(String tableName, String filename) throws DatabaseException {
        try {
            CallableStatement callableStatement = connection.prepareCall("call syscs_util.syscs_import_table(null, ?, ?, null, null, 'UTF-8', 0)");
            callableStatement.setString(1, tableName);
            callableStatement.setString(2, filename);
            boolean result = callableStatement.execute();
            System.out.println(result ? "Successfully added the data to the table " + tableName : "Failed to add data to the table " + tableName);
        } catch(SQLException e) {
            throw new DatabaseException("Could not load in data into a  - " + e);
        }
    }

    public void ingestTables() throws DatabaseException {
        ingest("parkingTime", "parkingTime.csv");
        ingest("parkingBay", "parkingBay.csv");
        ingest("street", "street.csv");
    }

    public void initialiseTables() throws DatabaseException{
        try {
            FileReader fileReader = new FileReader("src/main/resources/derbyTables.sql");
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            for (int i = 0; i < SQL_STATEMENT_COUNT; i++) {
                String sql = bufferedReader.readLine();
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
            }
        } catch(FileNotFoundException e) {
            throw new DatabaseException("Could not find the SQL file - " + e);
        } catch (SQLException e) {
            throw new DatabaseException("Could not execute SQL Update statement - " + e);
        } catch (IOException e) {
            throw new DatabaseException("Something went wrong when reading a line from the SQL file - " + e);

        }
    }

    /**
     * Calls the exit command
     *
     * @throws DatabaseException
     */
    public void exit() throws DatabaseException {

    }
}

//public class MongoDB extends Database {
//    public void ingest(String[] fields) {
//        System.out.println("ingesting into mongo");
//    }
//}


class DatabaseException extends Exception {
    private String message;

    /**
     *
     *
     * @param message
     */
    DatabaseException(String message) {
        this.message = message;
    }

    /**
     *
     *
     * @return
     */
    public String toString() {
        return message;
    }
}

abstract class Database {
    Connection connection;

    Database(String url) throws DatabaseException {
        this.connection = connect(url);
        System.out.println(url);
    }

    /**
     * ingest the number of records into the database from the file.
     *
     * @param tableName  The list of values from a record.
     *
     * @throws DatabaseException  An issue with the database occured.
     */
    public abstract void ingest(String tableName, String filename) throws DatabaseException;



    /**
     * Makes a connection to the database and returns the connection.
     *
     * @param url The URL of the database
     * @throws DatabaseException thrown if connection could not be made.
     */
    private Connection connect(String url) throws DatabaseException {
        Connection connection;
        try {
            connection = DriverManager.getConnection(url);
        }
        catch(SQLException e) {
            throw new DatabaseException("Could not connect to the database - " + e);
        }

        return connection;
    }
}