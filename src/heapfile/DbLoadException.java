package heapfile;


/**
 * An exception that is thrown by DbLoad instance.
 */
public class DbLoadException extends Exception {
    private String message;

    /**
     * Creates a DbLoad exception message.
     */
    DbLoadException(String message) {
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