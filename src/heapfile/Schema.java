package heapfile;

class Schema {
    private static final int BYTES_IN_CHAR = 2;


    /**
     * The fields and their sizes for the csv data file to make into a heapfile.
     */
    static final Field[] fields = new Field[]{
            new Field(30 * BYTES_IN_CHAR, "string"),
            new Field(4, "int"),
            new Field(4, "timestamp"),
            new Field(4, "timestamp"),
            new Field(8, "long"),
            new Field(6 * BYTES_IN_CHAR, "string"),
            new Field(50 * BYTES_IN_CHAR, "string"),
            new Field(20 * BYTES_IN_CHAR, "string"),
            new Field(4, "int"),
            new Field(35 * BYTES_IN_CHAR, "string"),
            new Field(35 * BYTES_IN_CHAR, "string"),
            new Field(35 * BYTES_IN_CHAR, "string"),
            new Field(4, "int"),
            new Field(1, "boolean")
    };

    public enum FIELDS {
        DA_NAME,
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
     * Gets the total record size by adding all the size together.
     *
     * @return the record size.
     */
    static int getTotalRecordSize() {
        int count = 0;
        for (Field field : fields) {
            count += field.size;
        }
        return count;
    }

    /**
     * returns the offset of the field in a record.
     *
     * @param field The field to get the offset for.
     * @return  The offset of the field.
     */
    static int getOffset(FIELDS field) {
        int offset = 0;
        for (int i = 0; i < field.ordinal(); i++) {
            offset += fields[i].size;
        }

        return offset;
    }

    /**
     * Gets the size of the field specified.
     */
    static int getSize(FIELDS field) {
        return fields[field.ordinal()].size;
    }
}
