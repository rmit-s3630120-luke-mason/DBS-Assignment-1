package heapfile;

import java.io.*;
import java.util.Date;
import java.util.Properties;

import static heapfile.HeapFileConfigProvider.getHeapfileConfigProvider;

public class dbload {
    public enum FIELDS {
        DEVICE_ID,
        ARRIVAL_TIME,
        DEPARTURE_TIME,
        DURATION_SECONDS,
        STREET_MARKER,
        SIGN,
        AREA,
        STREET_ID,
        STREET_NAME,
        BETWEEN_STREET_1,
        BETWEEN_STREET_2,
        SIDE_OF_STREET,
        IN_VIOLATION
    }

    public static void main(String[] args) {

        // Check for bad arguments
        if (args.length != 3) {
            System.out.println("Not enough arguments: dbload [-p] [pagesize] [datafile]");
            return;
        }

        // TODO make it so that the arg that comes after -p is the pagesize
        if (!args[0].equalsIgnoreCase("-p")) {
            System.out.println("Argument -p was not given");
            return;
        }

        int pagesize;
        try {
            pagesize = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("ERROR: Page size given is invalid - " + e);
        }

        File file = new File(args[2]);
        if (!file.exists()) {
            System.out.println("ERROR: File specified to read form does not exist");
            return;
        }



        // TODO output heapfile heap.4096
        // TODO Write to output file in buffer size 4096
        // Write each line of csv to heapfile file.
        // If next record cannot fit in the left over page size, pad the page and keep writing a new page


        System.out.println("Number of records loaded: ");
        System.out.println("Number of pages used:");
        System.out.println("Time taken to create heap file:  ms");
    }

    private void createHeapFile(String file, int pageSize) throws DbLoadException {
        FileReader fileReader;
        try {
            fileReader = new FileReader(file);
        } catch (FileNotFoundException e) {
            throw new DbLoadException("File specified was not found - " + e);
        }

        // Load in the property file containing the file paths
        Properties properties = getHeapfileConfigProvider().getPropertyFile(HeapFileConfigProvider.HEAP_FILE_CONFIG);

        // Read from the data file line by line
        try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            try (FileOutputStream fileOutputStream = new FileOutputStream(properties.getProperty("heapFileLocation") + "heap." + pageSize)) {

//                fileOutputStream.write();

                // Skip over the header line
                bufferedReader.readLine();

                String line;
                int lines = 0;

                int maxRecordsPerPage = pageSize % Sizes.getTotalRecordSize();
                byte[] page = new byte[pageSize];

                // split each line
                while((line = bufferedReader.readLine()) != null) {
                    String[] values = line.split(",");


                    if (values.length == FIELDS.values().length) {
                        if (lines % maxRecordsPerPage == 0 && lines != 0) {
                            // TODO write page to file
//                            writePageToHeapFile(page, fileOutputStream);
                            page = new byte[pageSize];
                        }

//                        addRecordToPage(page, values);
                        if (lines % 10000 == 0 && lines != 0) {
                            System.out.println("Lines Written: " + lines);
                        }
                    } else {
                        throw new DbLoadException("File given does not contain the correct number of fields");
                    }
                    lines++;
                }
            }
        } catch (IOException e) {
            throw new DbLoadException("There was an issue reading or writing with the file - " + e);
        }
    }

    /**
     * Writes a record to the heap file.
     *
     * @param values
     * @throws DbLoadException
     */
    private void writePageToHeapFile(String[] values, byte[] page, FileOutputStream fileOutputStream) throws DbLoadException {

        byte[] duration = new byte[4];
        byte[] streetMarker = new byte[4];
        byte[] signDetails = new byte[4];
        byte[] area = new byte[4];
        byte[] streetId = new byte[4];
        byte[] streetName = new byte[35];
        byte[] betweenStreet1 = new byte[4];
        byte[] betweenStreet2 = new byte[4];
        byte[] sideOfStreet = new byte[4];
        byte[] inViolation = new byte[4];

        String deviceIdStr = values[FIELDS.DEVICE_ID.ordinal()];
        String arrivalTimeStr = values[FIELDS.ARRIVAL_TIME.ordinal()];

        String DA_NAME = deviceIdStr + "-" + arrivalTimeStr;
        int deviceId = Integer.parseInt(deviceIdStr);
        byte[] arrivalTime = getTimestampBytes(arrivalTimeStr);
        byte[] departureTime = getTimestampBytes(values[FIELDS.DEPARTURE_TIME.ordinal()]);

//        values[FIELDS.DURATION_SECONDS.ordinal()]
//        values[FIELDS.IN_VIOLATION.ordinal()]
//        values[FIELDS.STREET_MARKER.ordinal()]
//        values[FIELDS.SIGN.ordinal()]
//        values[FIELDS.STREET_ID.ordinal()]
//        values[FIELDS.SIDE_OF_STREET.ordinal()]
//        values[FIELDS.STREET_NAME.ordinal()]
//        values[FIELDS.BETWEEN_STREET_1.ordinal()]
//        values[FIELDS.BETWEEN_STREET_2.ordinal()]
//        values[FIELDS.AREA.ordinal()]
    }

    /**
     * Gets the 4 byte timestamp value from the date string.
     *
     * @param dateStr  The date string
     * @return  4 byte timestamp array.
     */
    private static byte[] getTimestampBytes(String dateStr) {
        Date date = new Date(dateStr);
        int timeInSec = (int)date.getTime() / 1000;

        return new byte[]{
                (byte) (timeInSec >> 24),
                (byte) (timeInSec >> 16),
                (byte) (timeInSec >> 8),
                (byte) timeInSec
        };
    }

    /**
     *
     * @return
     */
    private static byte[] getIntBytes(String value) {
        int number = Integer.parseInt(value);

    }
}
