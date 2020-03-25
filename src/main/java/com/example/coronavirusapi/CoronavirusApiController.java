package com.example.coronavirusapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class CoronavirusApiController {
    private final Map<String, Map<String, Map<String, Map<String, String>>>> data;

    /**
     * An enum for specifying if a coronavirus case
     * is a confirmed case, a death or a recovery.
     */
    private enum CaseType {CONFIRMED, DEATHS, RECOVERED}

    public CoronavirusApiController() {
        data = new HashMap<>();
        processCsvResource(
                data,
                "csse_covid_19_data/csse_covid_19_time_series/" +
                        "time_series_19-covid-Confirmed.csv",
                CaseType.CONFIRMED
        );
        processCsvResource(
                data,
                "csse_covid_19_data/csse_covid_19_time_series/" +
                        "time_series_19-covid-Deaths.csv",
                CaseType.DEATHS
        );
        processCsvResource(
                data,
                "csse_covid_19_data/csse_covid_19_time_series/" +
                        "time_series_19-covid-Recovered.csv",
                CaseType.RECOVERED
        );
    }

    private static void processCsvResource(
            Map<String, Map<String, Map<String, Map<String, String>>>> out,
            String path,
            CaseType caseType) {
        Iterator<Map<String, String>> it = readCsvResource(path);
        String caseName = caseType.name().toLowerCase();

        while (it.hasNext()) {
            Map<String, String> row = it.next();
            String country = row.remove("Country/Region");
            String province = row.remove("Province/State");
            // If `province` is an empty string, `row` describes the entire country
            province = (province.isEmpty()) ? "all" : province;
            // Omit lat/lon
            row.remove("Lat");
            row.remove("Long");

            out.putIfAbsent(country, new HashMap<>());
            out.get(country).putIfAbsent(province, new HashMap<>());

            // Only date entries remain at this point
            for (String date : row.keySet()) {
                String cases = row.get(date);
                out.get(country).get(province).putIfAbsent(date, new HashMap<>());
                out.get(country).get(province).get(date).put(caseName, cases);
            }
        }
    }

    private static Iterator<Map<String, String>> readCsvResource(String path) {
        try {
            ObjectMapper mapper = new CsvMapper();
            CsvSchema schema = CsvSchema.emptySchema().withHeader();
            File f = new ClassPathResource(path).getFile();

            return mapper.readerFor(Map.class)
                    .with(schema)
                    .readValues(f);
        } catch (IOException e) {
            return Collections.emptyIterator();
        }
    }

    @GetMapping("/api/all")
    public Map<String, Map<String, Map<String, Map<String, String>>>>
    all(@RequestParam(value = "date", defaultValue = "all") String queryDate,
        @RequestParam(value = "country", defaultValue = "all") String queryCountry,
        @RequestParam(value = "state", defaultValue = "all") String queryState) {

        if (!queryDate.equals("all"))
            queryDate = adjustDateFormat(queryDate);

        Map<String, Map<String, Map<String, Map<String, String>>>> result = new HashMap<>();

        if (data.containsKey(queryCountry)) {
            if (data.get(queryCountry).containsKey(queryState)) {
                addEntries(queryCountry, queryState, queryDate, result);
            } else if (queryState.equals("all")) {
                for (String state : data.get(queryCountry).keySet()) {
                    addEntries(queryCountry, state, queryDate, result);
                }
            }
        } else if (queryCountry.equals("all")){
            for (String country : data.keySet()) {
                for (String state : data.get(country).keySet()) {
                    addEntries(country, state, queryDate, result);
                }
            }
        }
        return result;
    }
    // accepts a date in MMDDYYYY format and returns it in M/DD/YY format.
    // If the date is invalid, returns an empty string.
    private String adjustDateFormat(String date) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat("MMddyyyy");
            Date newDate = formatter.parse(date);

            return new SimpleDateFormat("M/d/yy").format(newDate);
        } catch (ParseException e) {
            return "";
        }
    }

    private void addEntries(String country, String state, String queryDate,
                            Map<String, Map<String, Map<String, Map<String, String>>>> result)  {
        if (data.get(country).get(state).keySet().contains(queryDate)) {
            Map<String, String> dateMap = data.get(country).get(state).get(queryDate);
            result.putIfAbsent(country, new HashMap<>());
            result.get(country).putIfAbsent(state, new HashMap<>());
            result.get(country).get(state).put(queryDate, dateMap);
        } else if (queryDate.equals("all")) {
            for (String date : data.get(country).get(state).keySet()) {
                Map<String, String> dateMap = data.get(country).get(state).get(date);
                result.putIfAbsent(country, new HashMap<>());
                result.get(country).putIfAbsent(state, new HashMap<>());
                result.get(country).get(state).put(date, dateMap);
            }
        }
    }
    @GetMapping("/api/confirmed")
    public String confirmed(
            @RequestParam(value = "date") String date,
            @RequestParam(value = "location") String location) {
        return null;
    }

    @GetMapping("/api/deaths")
    public String deaths(
            @RequestParam(value = "date") String date,
            @RequestParam(value = "location") String location) {
        return null;
    }

    @GetMapping("api/recovered")
    public String recovered(
            @RequestParam(value = "date") String date,
            @RequestParam(value = "location") String location) {
        return null;
    }


}
