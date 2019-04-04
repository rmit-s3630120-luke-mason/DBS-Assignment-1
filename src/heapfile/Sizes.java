package heapfile;

public class Sizes {
    public static final int DA_NAME = 30;
    public static final int DEVICE_ID = 4;
    public static final int ARRIVAL_TIME = 4;
    public static final int DEPARTURE_TIME = 4;
    public static final int DURATION_SECONDS = 8;
    public static final int STREET_MARKER = 6;
    public static final int SIGN = 50;
    public static final int AREA = 20;
    public static final int STREET_ID = 4;
    public static final int STREET_NAME = 35;
    public static final int BETWEEN_STREET_1 = 35;
    public static final int BETWEEN_STREET_2 = 35;
    public static final int SIDE_OF_STREET = 4;
    public static final int IN_VIOLATION = 1;

    public static int getTotalRecordSize() {
        return DA_NAME +
                DEVICE_ID +
                ARRIVAL_TIME +
                DEPARTURE_TIME +
                DURATION_SECONDS +
                STREET_MARKER +
                SIGN + AREA +
                STREET_ID +
                STREET_NAME +
                BETWEEN_STREET_1 +
                BETWEEN_STREET_2 +
                SIDE_OF_STREET +
                IN_VIOLATION;
    }
}
