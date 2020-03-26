package com.example.coronavirusapi;
import java.util.*;

import com.example.coronavirusapi.custom_exceptions.InvalidDateFormatException;
import com.example.coronavirusapi.custom_exceptions.InvalidStateException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class CoronavirusApiControllerAdvice extends ResponseEntityExceptionHandler {
    @ExceptionHandler(InvalidDateFormatException.class)
    public ResponseEntity<Map<String, String>> handleInvalidDateFormatException(
            InvalidDateFormatException ex) {
        return createResponse(ex);
    }

    @ExceptionHandler(InvalidStateException.class)
    public ResponseEntity<Map<String, String>> handleInvalidStateException(
            InvalidStateException ex) {
        return createResponse(ex);
    }

    /*
    @ExceptionHandler(NoHandlerFoundException.class)
    public ResponseEntity<Map<String, String>> requestHandlingNoHandlerFound(
            NoHandlerFoundException ex) {
        Map<String, String> body = Map.of("error", "request sent to invalid endpoint");
        return new ResponseEntity(body, HttpStatus.NOT_FOUND);
    }*/

    private ResponseEntity<Map<String, String>> createResponse(Exception ex) {
        Map<String, String> body = Map.of("error", ex.getLocalizedMessage());
        return new ResponseEntity(body, HttpStatus.BAD_REQUEST);
    }
}


