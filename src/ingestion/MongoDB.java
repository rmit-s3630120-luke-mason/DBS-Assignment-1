package ingestion;

import java.io.*;
import java.util.Properties;

import static ingestion.ConfigProvider.getConfigProvider;

/**
 *
 */
class MongoDB {
    private Properties properties;

    MongoDB() throws DatabaseException {
        properties = getConfigProvider().getPropertyFile(ConfigProvider.MONGO_CONFIG);
        registerMongoDbService();
    }

    /**
     * Ingests the JSON file into mongoDB
     */
    void ingest() throws DatabaseException {
        try {
            Runtime.getRuntime().exec("mongoimport --db " +
                    properties.getProperty("dbName") +
                    " --collection " +
                    properties.getProperty("collectionName") +
                    " --file " +
                    properties.getProperty("jsonFileDestination") +
                    properties.getProperty("jsonFilename") +
                    " --jsonArray\n ");
        } catch(IOException e) {
            throw new DatabaseException("Command could not be run - " + e);
        }
    }

    /**
     * Start the mongoDB servive so the mongo commands can be executed.
     *
     * @throws DatabaseException Thrown if the mongoDB cannot start the service.
     */
    private void registerMongoDbService() throws DatabaseException{
        try {
            Runtime.getRuntime().exec("mongod --config " + properties.getProperty("mongoRoot") + "mongod.conf");
        } catch(IOException e) {
            throw new DatabaseException("Command could not be run - " + e);
        }
    }
}
