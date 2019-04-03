package ingestion;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static ingestion.ConfigProvider.getConfigProvider;

/**
 *
 */
class DerbyDB {
    private Connection connection;

    private static final int SQL_STATEMENT_COUNT = 3;

    /**
     * Constructor that loads in the embedded derby driver.
     */
    DerbyDB(String url, String driver) throws DatabaseException {
        this.connection = connect(url);

        try {
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DatabaseException("Could not create derby driver - " + e);
        }
    }

    /**
     * Ingests the specified files into the derby DB database
     */
    void ingest() throws DatabaseException {
        Properties properties;

        // Get the csv file paths from properties file.
        properties = getConfigProvider().getPropertyFile("csvTables");

        // Ingest the csv files into the tables.
        try (Statement statement = connection.createStatement()) {
            statement.execute("call syscs_util.syscs_import_table(null, '" + DataSplitter.STREET + "', '" +
                    properties.getProperty(DataSplitter.STREET) + "', null, null, null, 0)");
            statement.execute("call syscs_util.syscs_import_table(null, '" + DataSplitter.PARKING_BAY + "', '" +
                    properties.getProperty(DataSplitter.PARKING_BAY) + "', null, null, null, 0)");
            statement.execute("call syscs_util.syscs_import_table(null, '" + DataSplitter.PARKING_TIME + "', '" +
                    properties.getProperty(DataSplitter.PARKING_TIME) + "', null, null, null, 0)");
        } catch(SQLException e) {
            throw new DatabaseException("Could not insert data into a table - " + e);
        }
    }

    /**
     * Initialises/creates the tables in the derby DB.
     *
     * @throws DatabaseException thrown if an issue occurred.
     */
    void initialiseTables() throws DatabaseException {
        try (FileReader fileReader = new FileReader("../config/derbyTables.sql")) {
            try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {
                for (int i = 0; i < SQL_STATEMENT_COUNT; i++) {
                    String sql = bufferedReader.readLine();
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                }
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
            e.printStackTrace();
            throw new DatabaseException("Could not connect to the database - " + e);
        }

        return connection;
    }
}

