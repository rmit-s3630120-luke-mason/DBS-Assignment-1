package ingestion;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigProvider {
    public static final String MONGO_CONFIG = "mongoConfig.properties";
    public static final String DERBY_CONFIG = "derbyConfig.properties";
    public static final String CSV_TABLES = "csvTables.properties";
    public static final String DERBY_TABLES = "derbyTables.sql";

    public static final String DERBY = "derby";
    public static final String MONGO = "mongo";

    private static ConfigProvider configProvider = null;
    private static String configDirectory = "../config/";
    private ConfigProvider() {

    }

    public static ConfigProvider getConfigProvider() {
        if (configProvider == null) {
            configProvider = new ConfigProvider();
        }

        return configProvider;
    }

    /**
     * Gets the property file and loads in the properties into the property object.
     *
     * @param propertyFileName  The property file to read.
     * @return  The Properties.
     * @throws DatabaseException thrown if an issue occurred in finding the file or loading in the properties in the file.
     */
    Properties getPropertyFile(String propertyFileName) throws DatabaseException {
        try (InputStream input = new FileInputStream(configDirectory + propertyFileName)) {
            Properties properties = new Properties();
            properties.load(input);
            return properties;
        } catch (FileNotFoundException e) {
            throw new DatabaseException("Could not find the '" + propertyFileName + "' file - " + e);
        } catch (IOException e) {
            throw new DatabaseException("Could not load in '" + propertyFileName + "' file - " + e);
        }
    }
}
