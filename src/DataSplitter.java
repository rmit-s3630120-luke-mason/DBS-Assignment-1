import java.io.*;
import java.util.Arrays;

/**
 * Splits a csv ile containing all the data, into separate csv files that represent each table of data in the derbyDB
 */
public class DataSplitter {
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

    /**
     * Splits data in the file specified into other files.
     *
     * @param args [filename to split, amount of data lines to read]
     */
    public static void main(String[] args) throws IOException {
        File f1 = new File("parkingTime.csv");
        File f2 = new File("parkingBay.csv");
        File f3 = new File("street.csv");

        f1.createNewFile();
        f2.createNewFile();
        f3.createNewFile();

        FileWriter fw1 = new FileWriter(f1);
        FileWriter fw2 = new FileWriter(f2);
        FileWriter fw3 = new FileWriter(f3);

        // Get the file
        File file = new File(args[0]);
        if (!file.exists()) {
            System.out.println("File does not exist - " + file.getAbsolutePath());
            return;
        }

        split(file, Integer.parseInt(args[1]), fw1, fw2, fw3);

        fw1.close();
        fw2.close();
        fw3.close();
    }

    /**
     * ingest the number of records into the database from the file.
     *
     * @param file
     * @param records
     * @param f1
     * @param f2
     * @param f3
     * @throws IOException The file could not be read from.
     */
    private static void split(File file, int records, FileWriter f1, FileWriter f2, FileWriter f3)
            throws IOException {
        try (FileReader fileReader = new FileReader(file)) {
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            // Skip over the header line
            bufferedReader.readLine();

            // split each line
            for (int i = 0; i < records; i++) {
                String line = bufferedReader.readLine();
                if (line == null) {
                    System.out.println("End of file reached, can't read anymore lines. Line reached = " + i);
                    break;
                }

                String[] values = line.split(",");
                if (values.length == 13) {// TODO get rid of this magic number
                    writeValues(values, f1, f2, f3);
                } else {
                    System.out.println(Arrays.toString(values) + " - was not included");
                }
            }
        }
    }

    /**
     * @param values
     * @param f1
     * @param f2
     * @param f3
     */
    private static void writeValues(String[] values, FileWriter f1, FileWriter f2, FileWriter f3) throws IOException {

        // Allocating split record to 3 different records as string to be written.
        String table1Record =
                values[FIELDS.DEVICE_ID.ordinal()] + "," +
                        values[FIELDS.ARRIVAL_TIME.ordinal()] + "," +
                        values[FIELDS.DEPARTURE_TIME.ordinal()] + "," +
                        values[FIELDS.DURATION_SECONDS.ordinal()] + "," +
                        values[FIELDS.IN_VIOLATION.ordinal()] + "," +
                        values[FIELDS.STREET_MARKER.ordinal()] + "\n";

        String table2Record =
                values[FIELDS.STREET_MARKER.ordinal()] + "," +
                        values[FIELDS.SIGN.ordinal()] + "," +
                        values[FIELDS.STREET_ID.ordinal()] + "," +
                        values[FIELDS.SIDE_OF_STREET.ordinal()] + "," +
                        values[FIELDS.STREET_NAME.ordinal()] + "\n";

        String table3Record =
                values[FIELDS.STREET_NAME.ordinal()] + "," +
                        values[FIELDS.BETWEEN_STREET_1.ordinal()] + "," +
                        values[FIELDS.BETWEEN_STREET_2.ordinal()] + "," +
                        values[FIELDS.AREA.ordinal()] + "\n";

        // Writing the split record into three files
        f1.write(table1Record);
        f2.write(table2Record);
        f3.write(table3Record);
    }
}
