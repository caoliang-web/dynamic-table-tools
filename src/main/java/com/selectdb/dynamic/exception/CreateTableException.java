package com.selectdb.dynamic.exception;

/** Create Table exception. */
public class CreateTableException extends RuntimeException {
    public CreateTableException() {
        super();
    }

    public CreateTableException(String message) {
        super(message);
    }

    public CreateTableException(String message, Throwable cause) {
        super(message, cause);
    }
}
