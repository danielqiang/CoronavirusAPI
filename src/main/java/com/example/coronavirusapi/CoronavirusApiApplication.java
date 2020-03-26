package com.example.coronavirusapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class CoronavirusApiApplication {
    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(CoronavirusApiApplication.class, args);
    }
}
