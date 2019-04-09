package heapfile;

import java.io.*;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import static heapfile.dbload.CHARSET;

/**
 * Makes a saerch query on a heapfile.
 */
public class dbquery {
    private static int recordsFound = 0;

    public static void main(String[] args) {

        // Check for bad arguments
        if (args.length != 2) {
            System.out.println("Not enough arguments: dbquery [text] [pagesize]");
            System.out.println(args.length + " arguments specified, expected 2");
            return;
        }

        long start = new Date().getTime();
        try {
            int pageSize = stringToInt(args[1]);
            File heapfile = getHeapFile(pageSize);
            String text = args[0];
            search(heapfile, pageSize, text);
        } catch (Exception e) {
            System.out.println("ERROR: " + e);
        }

        long end = new Date().getTime();

        System.out.println("Time taken: " + (end - start) + " ms");
        System.out.println("The amount of records found that match: " + recordsFound);
    }

    /**
     * Converts string to Int.
     *
     * @return The int.
     * @throws DbQueryException If the string could not be converted to a integer.
     */
    private static int stringToInt(String value) throws DbQueryException {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new DbQueryException("Could not parse the int to string - " + e);
        }
    }

    /**
     * Gets the heap file with the given pageSize.
     *
     * @param pageSize The page size
     * @return The heapfile.
     * @throws DbQueryException throws if the file does not exist.
     * @throws DbLoadException thrown if the config could not be loaded.
     */
    private static File getHeapFile(int pageSize) throws DbQueryException, DbLoadException {

        // Get heapfile directory from config file.
        Properties properties = HeapFileConfigProvider.getHeapfileConfigProvider().getPropertyFile(HeapFileConfigProvider.HEAP_FILE_CONFIG);
        String location = properties.getProperty("heapFileLocation");

        // Get the from file path
        File file = new File(location + "heap." + pageSize);
        if (!file.exists()) {
            throw new DbQueryException("Heap file does not exist - " + location + "heap." + pageSize);
        }

        return file;
    }

    /**
     * Searches the file for the given text.
     *
     * @param file        The file to search in.
     * @param pageSize    The size of the page.
     * @param searchText  The text to search with.
     */
    private static void search(File file, int pageSize, String searchText) throws DbQueryException {
        try {
            FileInputStream inputStream = new FileInputStream(file);
            byte[] page = new byte[pageSize];
            while(inputStream.read(page, 0, pageSize) != -1) {
                searchPage(page, searchText);
            }
        } catch (FileNotFoundException e) {
            throw new DbQueryException("File could not be found - " + e);
        } catch (IOException e) {
            throw new DbQueryException("There was an issue when reading the file" + e);
        }


    }

    /**
     * Searches the page for the saerch text.
     *
     * @param page        The page to search in bytes.
     * @param searchText  The text to search.
     */
    private static void searchPage(byte[] page, String searchText) {
        int offset = 0;
        int numRecords = page.length / Schema.getTotalRecordSize();
        for (int i = 0; i < numRecords; i++) {
            byte[] record = Arrays.copyOfRange(page, offset, offset + Schema.getTotalRecordSize());

            searchRecord(record, searchText);
            offset+= Schema.getTotalRecordSize();
        }
    }

    /**
     * Searches the DA_NAME value for any matches in for the searchText.
     *
     * @param record      The record to Search in.
     * @param searchText  The text to search with.
     */
    private static void searchRecord(byte[] record, String searchText) {

        // Only search the first 30 bytes as the assignment is only meant to search in the DA_NAME value.
        byte[] txt = searchText.getBytes(CHARSET);
        byte[] daName = Arrays.copyOfRange(record, 0, 30);
        String field = new String(daName, CHARSET);
        searchText = new String(txt, CHARSET);
        if (field.contains(searchText)) {
            recordsFound++;
            System.out.println(field);
        }
    }
}
