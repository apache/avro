package org.apache.avro;

public class AvroFieldNotFound extends AvroTypeException {
    public AvroFieldNotFound(String message) {
        super(message);
    }

    public AvroFieldNotFound(String message, Throwable cause) {
        super(message, cause);
    }
}
