package ingestion;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class MongoDB {
    MongoDB() throws DatabaseException {
        registerMongoDbService();
    }

    /**
     * Ingests the JSON file into mongoDB
     */
    public void ingest() throws DatabaseException {
        try {
            Runtime.getRuntime().exec("mongoimport ");
        } catch(IOException e) {
            throw new DatabaseException("Command could not be run - " + e);
        }
    }

    /**
     * Creates a json file from the csv using a json structure from maps.
     *
     * @param fileDestination  The fileDestination for the JSON file be created, including the name of the file.
     */
    void createJSONFromCSVs(String fileDestination) throws DatabaseException {
        try {
            File jsonFile = new File(fileDestination);
            jsonFile.createNewFile();



        } catch (IOException e) {
            throw new DatabaseException("Could not create the json file - " + e);
        }

    }

    /**
     * Start the mongoDB servive so the mongo commands can be executed.
     *
     * @throws DatabaseException
     */
    private void registerMongoDbService() throws DatabaseException{
        try {
            Runtime.getRuntime().exec("mongod --config /usr/local/etc/mongod.conf");
        } catch(IOException e) {
            throw new DatabaseException("Command could not be run - " + e);
        }
    }
}
