package com.example.coronavirusapi.custom_exceptions;

public class InvalidDateFormatException extends IllegalArgumentException {
    public InvalidDateFormatException(String datePattern, Throwable cause) {
        super("Invalid date format. Must be in " + datePattern + " format", cause);
    }
}