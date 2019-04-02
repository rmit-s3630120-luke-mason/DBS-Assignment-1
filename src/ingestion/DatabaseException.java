package ingestion;

/**
 * An exception that is thrown by Database instances.
 */
public class DatabaseException extends Exception {
    private String message;

    /**
     * Creates a Database exception message.
     */
    DatabaseException(String message) {
        this.message = message;
    }

    /**
     * Turns message into a certain formatted message if needed.
     *
     * @return the exception message.
     */
    public String toString() {
        return message;
    }
}