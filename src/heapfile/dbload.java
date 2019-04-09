package heapfile;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Properties;

import static heapfile.HeapFileConfigProvider.getHeapfileConfigProvider;

/**
 * This program loads in the specified datafile into a heapfile.
 */
public class dbload {
    static final Charset CHARSET = StandardCharsets.UTF_8;
    private static int recordCount = 0;
    private static int pageCount = 0;

    public static void main(String[] args) {

        // Check for bad arguments
        if (args.length != 3) {
            System.out.println("Not enough arguments: dbload [-p] [pagesize] [datafile]");
            System.out.println(args.length + " arguments specified, expected 3");
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
            return;
        }

        File file = new File(args[2]);
        if (!file.exists()) {
            System.out.println("ERROR: File specified to read from does not exist");
            return;
        }

        long start = new Date().getTime();
        try {
            createHeapFile(file, pagesize);
        } catch(DbLoadException e) {
            System.out.println("ERROR: " + e);
            return;
        }

        long end = new Date().getTime();

        System.out.println("Number of records loaded: " + recordCount);
        System.out.println("Number of pages used: " + pageCount);
        System.out.println("Size of one record: " + Schema.getTotalRecordSize() +  " bytes");
        System.out.println("Time taken to create heap file: " + (end - start) + " ms");
    }

    /**
     * Creates a heap file from the dataFile specified.
     *
     * @param dataFile The data file to read data from.
     * @param pageSize The page size to write to the heap file with.
     * @throws DbLoadException thrown if an issue occures.
     */
    private static void createHeapFile(File dataFile, int pageSize) throws DbLoadException {
        FileReader fileReader;
        try {
            fileReader = new FileReader(dataFile);
        } catch (FileNotFoundException e) {
            throw new DbLoadException("File specified was not found - " + e);
        }

        // Load in the property file containing the file paths
        Properties properties = getHeapfileConfigProvider().getPropertyFile(HeapFileConfigProvider.HEAP_FILE_CONFIG);

        // Read from the data file line by line
        try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            try (FileOutputStream fileOutputStream = new FileOutputStream(properties.getProperty("heapFileLocation") + "heap." + pageSize)) {

                // Skip over the header line
                bufferedReader.readLine();

                String line;
                int lines = 0;

                int maxRecordsPerPage = pageSize / Schema.getTotalRecordSize();
                byte[] page = new byte[pageSize];

                int pageOffset = 0;

                // split each line
                while((line = bufferedReader.readLine()) != null) {
                    String[] values = line.split(",");

                    if (values.length != Schema.FIELDS.values().length - 1) {
                        throw new DbLoadException("File given does not contain the correct number of fields: " + values.length);
                    }

                    // Check if the page is full
                    if (lines % maxRecordsPerPage == 0 && lines != 0) {
                        writePageToHeapFile(page, fileOutputStream);
                        pageCount++;
                        page = new byte[pageSize];
                        pageOffset = 0;
                    }

                    // Add record to the page since it is not full yet.
                    addRecordToPage(page, values, pageOffset);
                    pageOffset+= Schema.getTotalRecordSize();
                    recordCount++;
                    lines++;
                }
                pageCount++;
                writePageToHeapFile(page, fileOutputStream);
            }
        } catch (IOException e) {
            throw new DbLoadException("There was an issue reading or writing with the file - " + e);
        }
    }

    /**
     * Adds a record of values to the page.
     * It is assumed that the record will fit in the page size with the given offset.
     *
     * @param page        The page size.
     * @param values      Te values to add to the page.
     * @param pageOffset  The offset of the values to add to the page.
     * @throws DbLoadException thrown if an issue occures.
     */
    private static void addRecordToPage(byte[] page, String[] values, int pageOffset) throws DbLoadException {

        // Get the bytes of the DA_NAME
        byte[] DA_NAME = (

                // The - 1 array pos is because the new field 'DA_NAME' takes up the first slot of enum so
                // everything else is shifted 1 element to right from original.
                values[Schema.FIELDS.DEVICE_ID.ordinal() - 1] + "-" +
                values[Schema.FIELDS.ARRIVAL_TIME.ordinal() - 1]
        ).getBytes();

        attachField(page, DA_NAME, pageOffset);

        int recordOffset = Schema.getSize(Schema.FIELDS.DA_NAME);
        int fieldSize;

        // For each field, Add it to the page but its FULL field type size, not just its value size.
        // Start i at 1 to skip the DA_NAME value
        for(int i = 1; i < Schema.FIELDS.values().length; i++) {
            String value = values[i - 1];
            Schema.FIELDS field = Schema.FIELDS.values()[i];

            // Holds actual size and value in field
            byte[] fieldValue;

            // Correctly serialise the specific string value into the correct dataType.
            // Each field has a different data type to be transposed into.
            switch(field) {
                case DEVICE_ID:
                    fieldSize = Schema.getSize(Schema.FIELDS.DEVICE_ID);
                    fieldValue = intToBytes(Integer.parseInt(value));
                    break;
                case STREET_ID:
                    fieldSize = Schema.getSize(Schema.FIELDS.STREET_ID);
                    fieldValue = intToBytes(Integer.parseInt(value));
                    break;
                case SIDE_OF_STREET:
                    fieldSize = Schema.getSize(Schema.FIELDS.SIDE_OF_STREET);
                    fieldValue = intToBytes(Integer.parseInt(value));
                    break;
                case STREET_NAME:
                    fieldSize = Schema.getSize(Schema.FIELDS.STREET_NAME);
                    fieldValue = value.getBytes(CHARSET);
                    break;
                case AREA:
                    fieldSize = Schema.getSize(Schema.FIELDS.AREA);
                    fieldValue = value.getBytes(CHARSET);
                    break;
                case BETWEEN_STREET_1:
                    fieldSize = Schema.getSize(Schema.FIELDS.BETWEEN_STREET_1);
                    fieldValue = value.getBytes(CHARSET);
                    break;
                case BETWEEN_STREET_2:
                    fieldSize = Schema.getSize(Schema.FIELDS.BETWEEN_STREET_2);
                    fieldValue = value.getBytes(CHARSET);
                    break;
                case SIGN:
                    fieldSize = Schema.getSize(Schema.FIELDS.SIGN);
                    fieldValue = value.getBytes(CHARSET);
                    break;
                case STREET_MARKER:
                    fieldSize = Schema.getSize(Schema.FIELDS.STREET_MARKER);
                    fieldValue = value.getBytes(CHARSET);
                    break;
                case ARRIVAL_TIME:
                    fieldSize = Schema.getSize(Schema.FIELDS.ARRIVAL_TIME);
                    fieldValue = getTimestampBytes(value);
                    break;
                case DEPARTURE_TIME:
                    fieldSize = Schema.getSize(Schema.FIELDS.DEPARTURE_TIME);
                    fieldValue = getTimestampBytes(value);
                    break;
                case DURATION_SECONDS:
                    fieldSize = Schema.getSize(Schema.FIELDS.DURATION_SECONDS);
                    fieldValue = longToBytes(Long.parseLong(value));
                    break;
                case IN_VIOLATION:
                    fieldSize = Schema.getSize(Schema.FIELDS.IN_VIOLATION);
                    fieldValue = new byte[booleanToByte(Boolean.parseBoolean(value))];
                    break;
                default: throw new DbLoadException("Could not find field for value given");
            }

            attachField(page, fieldValue, pageOffset + recordOffset);
            recordOffset+=fieldSize;
        }
    }

    /**
     * Writes the page to the file.
     *
     * @param page              The page to add to file.
     * @param fileOutputStream  The file output stream to write to file with.
     * @throws DbLoadException  thrown if there is an issue with writing to file.
     */
    private static void writePageToHeapFile(byte[] page, FileOutputStream fileOutputStream) throws DbLoadException {
        try {
            fileOutputStream.write(page);
        } catch(IOException e) {
            throw new DbLoadException("Something went wrong when writing page to file - " + e);
        }
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

        return intToBytes(timeInSec);
    }

    /**
     * Gets the 4 bytes values from an integer.
     *
     * @param value The int value.
     * @return The bytes that represent the integer.
     */
    private static byte[] intToBytes(int value) {
        return new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value};
    }

    /**
     * Gets the byte of a boolean.
     *
     * @param bool The boolean value
     * @return A byte representing the boolean value.
     */
    private static byte booleanToByte(boolean bool) {
        return (byte) (bool ? 1 : 0 );
    }


    /**
     * Converts the long to a byte array.
     *
     * @param x The long value.
     * @return The byte array.
     */
    private static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    /**
     * Attaches a field to the page
     *
     * @param page    The page to add record to.
     * @param field  The record to add.
     * @param offset  The offset of bytes in the page to start adding.
     */
    private static void attachField(byte[] page, byte[] field, int offset) {
        System.arraycopy(field, 0, page, offset, field.length);
    }
}
