package ingestion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

abstract class Database {
    Connection connection;

    Database(String url) throws DatabaseException {
        this.connection = connect(url);
        System.out.println(url);
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
