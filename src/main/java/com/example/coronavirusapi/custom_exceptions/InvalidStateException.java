package com.example.coronavirusapi.custom_exceptions;

public class InvalidStateException extends IllegalArgumentException {
    public InvalidStateException() {
        super("The `country` request parameter " +
                "may not equal 'total' if `state` param is provided.");
    }
}
