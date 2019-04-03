package ingestion;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static ingestion.ConfigProvider.getConfigProvider;

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

    private static Map<String, List<String>> mappedValues = new HashMap<>();
    static final String STREET = "STREET";
    static final String PARKING_BAY = "PARKINGBAY";
    static final String PARKING_TIME = "PARKINGTIME";

    /**
     * Splits data in the file specified into other files.
     *
     * @param args [filename to split, amount of data lines to read]
     */
    public static void main(String[] args) {
        int records;
        File dataFile;

        // Get the data file
        dataFile = new File(args[0]);
        if (!dataFile.exists()) {
            System.out.println("File does not exist - " + dataFile.getAbsolutePath());
            return;
        }

        // Get the record amount
        try {
            records = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Could not parse the amount: " + args[1] + " into a number");
            return;
        }

        try {
            // Extract data for derby or mongo formatting.
            if (args[0].equalsIgnoreCase(ConfigProvider.DERBY)) {

                    createDerbyData(dataFile, records);

            } else if (args[0].equalsIgnoreCase(ConfigProvider.MONGO)) {
                createMongoDbData(dataFile, records);
            } else {
                System.out.println("Arg 1 must be either 'derby' or 'mongo'");
            }
        } catch (DatabaseException e) {
            System.out.println("ERROR: " + e);
        }
    }

    /**
     * @param dataFile
     * @param records
     * @throws DatabaseException
     */
    private static void createDerbyData(File dataFile, int records) throws DatabaseException {
        Properties tables = getConfigProvider().getPropertyFile(ConfigProvider.DERBY_CONFIG);

        // Creating File objects for the csv files to write to.
        File f3 = new File(tables.getProperty(STREET));
        File f2 = new File(tables.getProperty(PARKING_BAY));
        File f1 = new File(tables.getProperty(PARKING_TIME));

        // Adding the lists that will hold the primary keys for the ingested records.
        // This is used to detect any duplicate primary keys in the records.
        mappedValues.put(STREET, new ArrayList<>());
        mappedValues.put(PARKING_BAY, new ArrayList<>());
        mappedValues.put(PARKING_TIME, new ArrayList<>());

        // Creating file writers from the file objects to use to write to the created files.
        try {
            FileWriter fw1 = new FileWriter(f1, false);
            FileWriter fw2 = new FileWriter(f2, false);
            FileWriter fw3 = new FileWriter(f3, false);

            // Splitting the main record up into three sets and writing the three sets to the three files.
            split(dataFile, records, fw1, fw2, fw3);

            // Closing the file writers
            fw1.close();
            fw2.close();
            fw3.close();
        } catch (IOException e) {
            throw new DatabaseException("IO Error occurred with the csv files - " + e);
        }
    }

    /**
     *
     *
     * @param dataFile
     * @param records
     */
    private static void createMongoDbData (File dataFile, int records) throws DatabaseException {
        Properties properties = getConfigProvider().getPropertyFile(ConfigProvider.MONGO_CONFIG);
        Map<String, Street> valuesMap = new HashMap<>();

        // Create the jsonFile to write to.
        try {
            String destination = properties.getProperty("jsonFileDestination");
            String filename = properties.getProperty("jsonFilename");
            String fileDestination = destination + filename;

            // Creates the Json File if it does note exist.
            // Used to write contents to the file.
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileDestination, false));

            // Read each record and write the record to the json file
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(dataFile))) {

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
                    if (values.length == FIELDS.values().length) { // TODO get rid of this magic number
                        writeMongoValues(values, valuesMap);
                    } else {
                        System.out.println(Arrays.toString(values) + " - was not included");
                    }
                }
            }


        } catch (IOException e) {
            throw new DatabaseException("Could not create the json file - " + e);
        }
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
    private static void split (File file,int records, FileWriter f1, FileWriter f2, FileWriter f3)
        throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {

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
                if (values.length == FIELDS.values().length) { // TODO get rid of this magic number
                    writeDerbyValues(values, f1, f2, f3);
                } else {
                    System.out.println(Arrays.toString(values) + " - was not included");
                }
            }
        }
    }

    private static void writeMongoValues(String[] values, Map<String, Street> map) {
        String streetPK = values[FIELDS.STREET_NAME.ordinal()];

        if (map.containsKey(streetPK)) {
            Street street = map.get(streetPK);
        } else {
            map.put(streetPK, );
        }
    }

    /**
     * @param values
     * @param f1
     * @param f2
     * @param f3
     */
    private static void writeDerbyValues(String[]values, FileWriter f1, FileWriter f2, FileWriter f3) throws IOException
    {

        // Allocating split record to 3 different records as string to be written.
        String arrivalTime = formatDate(values[FIELDS.ARRIVAL_TIME.ordinal()]);
        String parkingTimeRecord =
                values[FIELDS.DEVICE_ID.ordinal()] + "," +
                        arrivalTime + "," +
                        formatDate(values[FIELDS.DEPARTURE_TIME.ordinal()]) + "," +
                        values[FIELDS.DURATION_SECONDS.ordinal()] + "," +
                        values[FIELDS.IN_VIOLATION.ordinal()] + "," +
                        values[FIELDS.STREET_MARKER.ordinal()] + "\n";

        String parkingTimePK = values[FIELDS.DEVICE_ID.ordinal()] + "-" + arrivalTime;
        if (!mappedValues.get(PARKING_TIME).contains(parkingTimePK)) {
            mappedValues.get(PARKING_TIME).add(parkingTimePK);
            f1.write(parkingTimeRecord);
        }

        String parkingBayRecord =
                values[FIELDS.STREET_MARKER.ordinal()] + "," +
                        values[FIELDS.SIGN.ordinal()] + "," +
                        values[FIELDS.STREET_ID.ordinal()] + "," +
                        values[FIELDS.SIDE_OF_STREET.ordinal()] + "," +
                        values[FIELDS.STREET_NAME.ordinal()] + "\n";

        if (!mappedValues.get(PARKING_BAY).contains(values[FIELDS.STREET_MARKER.ordinal()])) {
            mappedValues.get(PARKING_BAY).add(values[FIELDS.STREET_MARKER.ordinal()]);
            f2.write(parkingBayRecord);
        }

        String streetRecord =
                values[FIELDS.STREET_NAME.ordinal()] + "," +
                        values[FIELDS.BETWEEN_STREET_1.ordinal()] + "," +
                        values[FIELDS.BETWEEN_STREET_2.ordinal()] + "," +
                        values[FIELDS.AREA.ordinal()] + "\n";

        if (!mappedValues.get(STREET).contains(values[FIELDS.STREET_NAME.ordinal()])) {
            mappedValues.get(STREET).add(values[FIELDS.STREET_NAME.ordinal()]);
            f3.write(streetRecord);
        }
    }

    private static String formatDate (String dateStr){
        Date lol = new Date(dateStr);
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return dateFormatter.format(lol);
    }
}

class Street {
    public String streetName;
    public String betweenStreet1;
    public String betweenStreet2;
    public String area;
    public Map<String, ParkingBay> parkingBays;

    Street(String streetName, String betweenStreet1, String betweenStreet2, String area) {
        this.streetName = streetName;
        this.betweenStreet1 = betweenStreet1;
        this.betweenStreet2 = betweenStreet2;
        this.area = area;
        this.parkingBays = new HashMap<>();
    }
}

class ParkingBay {
    public String streetMarker;
    public String signDetails;
    public int streetId;
    public int sideOfStreet;
    public Map<String, ParkingTime> parkingTimes;

    ParkingBay(String streetMarker, String signDetails, int streetId, int sideOfStreet) {
        this.streetMarker = streetMarker;
        this.signDetails = signDetails;
        this.streetId = streetId;
        this.sideOfStreet = sideOfStreet;
        this.parkingTimes = new HashMap<>();
    }
}

class ParkingTime {
    public int deviceId;
    public String arrivalTime;
    public String departureTime;
    public int duration;
    public boolean inViolation;

    ParkingTime(int deviceId, String arrivalTime, String departureTime, int duration, boolean inViolation) {
        this.deviceId = deviceId;
        this.arrivalTime = arrivalTime;
        this.departureTime = departureTime;
        this.duration = duration;
        this.inViolation = inViolation;
    }
}
