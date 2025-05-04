package com.example.perfermanceTest.FaultTolerance;

public class CanNotProcessItemException extends RuntimeException{

    /**
     * Default constructor.
     */
    public CanNotProcessItemException() {
        super();
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public CanNotProcessItemException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public CanNotProcessItemException(String message, Throwable cause) {
        super(message, cause);
    }

    public CanNotProcessItemException(Throwable cause) {
        super(cause);
    }
}
