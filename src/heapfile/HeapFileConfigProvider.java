package heapfile;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

class HeapFileConfigProvider {

    // This is the file name of a properties file in the config directory for the program to reference.
    static final String HEAP_FILE_CONFIG = "heapfile.properties";

    // The directory that this config provider reads from to location config files.
    private static final String CONFIG_DIRECTORY = "../config/";

    private static HeapFileConfigProvider configProvider = null;
    private HeapFileConfigProvider() {}

    /**
     * Creates the heapfile configProvider or retrieves it if it has already been created.
     *
     * @return The heapfile config provider
     */
    static HeapFileConfigProvider getHeapfileConfigProvider() {
        if (configProvider == null) {
            configProvider = new HeapFileConfigProvider();
        }

        return configProvider;
    }

    /**
     * Gets the property file and loads in the properties into the property object.
     *
     * @param propertyFileName  The property file to read.
     * @return  The Properties.
     * @throws DbLoadException thrown if an issue occurred in finding the file or loading in the properties in the file.
     */
    Properties getPropertyFile(String propertyFileName) throws DbLoadException {
        try (InputStream input = new FileInputStream(CONFIG_DIRECTORY + propertyFileName)) {
            Properties properties = new Properties();
            properties.load(input);
            return properties;
        } catch (FileNotFoundException e) {
            throw new DbLoadException("Could not find the '" + propertyFileName + "' file - " + e);
        } catch (IOException e) {
            throw new DbLoadException("Could not load in '" + propertyFileName + "' file - " + e);
        }
    }
}
