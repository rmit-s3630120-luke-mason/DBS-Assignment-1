package ingestion;

import java.io.*;
import java.util.Properties;

import static ingestion.ConfigProvider.getConfigProvider;

/**
 *
 */
class MongoDB {
    private String jsonFileDestination = null;
    private Properties properties;

    MongoDB() throws DatabaseException {
        properties = getConfigProvider().getPropertyFile(ConfigProvider.MONGO_CONFIG);
        registerMongoDbService();
    }

    /**
     * Ingests the JSON file into mongoDB
     */
    void ingest() throws DatabaseException {
        if (jsonFileDestination == null) {
            throw new DatabaseException("Cannot ingest json data because the json data has not been generated");
        }

        try {
            Runtime.getRuntime().exec("mongoimport ");
        } catch(IOException e) {
            throw new DatabaseException("Command could not be run - " + e);
        }
    }

    /**
     * Creates a json file from the csv using a json structure from maps.
     */
    void createJSONFromCSVs() throws DatabaseException {


    }

    private void writeRecordAsJSON(File file) {

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
