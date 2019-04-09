package heapfile;

/**
 * Dataclass for a field.
 */
class Field {
    int size;
    private String type;
    Field(int size, String type) {
        this.size = size;
        this.type = type;
    }
}
