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
import java.util.stream.Collectors;
import java.util.*;

@RestController
public class CoronavirusApiController {
    private final Map<String, Map<String, Map<String, Map<String, Integer>>>> data;

    /**
     * Enum for classifying a coronavirus case as a
     * confirmed case, a death or a recovery.
     */
    private enum CaseType {CONFIRMED, DEATHS, RECOVERED}

    public CoronavirusApiController() {
        // Pre-process CSV data
        data = new HashMap<>();
        loadCsvResource(
                data,
                "csse_covid_19_data/csse_covid_19_time_series/" +
                        "time_series_19-covid-Confirmed.csv",
                CaseType.CONFIRMED
        );
        loadCsvResource(
                data,
                "csse_covid_19_data/csse_covid_19_time_series/" +
                        "time_series_19-covid-Deaths.csv",
                CaseType.DEATHS
        );
        loadCsvResource(
                data,
                "csse_covid_19_data/csse_covid_19_time_series/" +
                        "time_series_19-covid-Recovered.csv",
                CaseType.RECOVERED
        );
    }

    private static void loadCsvResource(
            Map<String, Map<String, Map<String, Map<String, Integer>>>> out,
            String path,
            CaseType caseType) {
        Iterator<Map<String, String>> it = readCsvResource(path);
        String caseName = caseType.name().toLowerCase();

        while (it.hasNext()) {
            Map<String, String> row = it.next();
            String country = row.remove("Country/Region");
            String state = row.remove("Province/State");
            // If `state` is an empty string, `row` describes the entire country
            state = (state.isEmpty()) ? "all" : state;
            // Omit lat/lon
            row.remove("Lat");
            row.remove("Long");

            out.putIfAbsent(country, new HashMap<>());
            out.get(country).putIfAbsent(state, new HashMap<>());

            // Only date entries remain at this point
            for (String date : row.keySet()) {
                int cases = Integer.parseInt(row.get(date));
                out.get(country).get(state).putIfAbsent(date, new HashMap<>());
                out.get(country).get(state).get(date).put(caseName, cases);
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

    /**
     * Helper method to reformat a {@code String} date from
     * `MMddyyyy` format to `M/d/yy` format.
     * If the date is invalid, returns an empty string.
     */
    private static String reformatDate(String date) {
        try {
            SimpleDateFormat oldPattern = new SimpleDateFormat("MMddyyyy");
            SimpleDateFormat newPattern = new SimpleDateFormat("M/d/yy");

            return newPattern.format(oldPattern.parse(date));
        } catch (ParseException e) {
            return "";
        }
    }

    /**
     * Helper method to query internal {@code data} map.
     *
     * @param caseType {@code CaseType} enum describing the case type
     *                 (confirmed, deaths, recovered) to match.
     *                 If null, matches all case types.
     * @return {@code Map} containing all entries matching the query.
     */
    private Map<String, Map<String, Map<String, Map<String, Integer>>>>
    query(String date, String country, String state, CaseType caseType) {
        String formattedDate = (date.equals("all")) ? "all" : reformatDate(date);
        String caseName = (caseType == null) ? null : caseType.name().toLowerCase();

        // This method looks hefty because it's basically trying
        // to use `data` as a database. When we start using an actual database, this
        // code will likely be deprecated since SQL does all of this for us
        // and much more.
        return data.entrySet().stream()
                // Top level (keys are countries, values are {state: {...}} maps).
                // Filter out all countries that aren't equal to `country`.
                // Don't filter out any countries if `country` is "all".
                .filter(e -> (e.getKey().equalsIgnoreCase(country)
                        || country.equals("all")))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().entrySet().stream()
                                // Next level (keys are states, values are {date: {...}} maps).
                                // Filter out all states that aren't equal to `state`.
                                // Don't filter out any states if `state` is "all".
                                .filter(e1 -> (e1.getKey().equalsIgnoreCase(state)
                                        || state.equals("all")))
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e1 -> e1.getValue().entrySet().stream()
                                                // Next level (keys are dates, values are
                                                // {confirmed: x, deaths: y, recovered: z}.
                                                // Filter out all dates that aren't equal to `date`.
                                                // Don't filter out any dates if `date` is "all".
                                                .filter(e2 -> (e2.getKey().equals(formattedDate)
                                                        || formattedDate.equals("all")))
                                                .collect(Collectors.toMap(
                                                        Map.Entry::getKey,
                                                        e2 -> e2.getValue().entrySet().stream()
                                                                // Lowest level (keys are
                                                                // "confirmed", "deaths",
                                                                // "recovered", values are
                                                                // integers).
                                                                // Filter out all case types that
                                                                // aren't equal to `caseType`.
                                                                // Don't filter out anything
                                                                // if `caseType` is null.
                                                                .filter(e3 -> e3.getKey().equals(caseName)
                                                                        || caseName == null)
                                                                .collect(Collectors.toMap(
                                                                        Map.Entry::getKey,
                                                                        Map.Entry::getValue
                                                                        )
                                                                )
                                                        )

                                                )
                                        )
                                ).entrySet().stream()
                                // Filter out state entries with empty data (state: {}).
                                // This can happen if `date` is not found in `data`
                                // and all countries/states are matched.
                                .filter(e1 -> (!e1.getValue().isEmpty()))
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue
                                        )
                                )
                        )

                ).entrySet().stream()
                // Filter out country entries with empty data (country: {}).
                // This can happen if `state` or `date` is not found in `data`
                // and all countries are matched.
                .filter(e -> (!e.getValue().isEmpty()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                        )
                );
    }

    // TODO: Issues to fix:
    //  - Some (all?) countries with explicitly named provinces/states do not have
    //    an 'all' key.
    //  - Normalize returned date to MMddyyyy so sorting them lexicographically
    //    works
    //  - Design question: how can we support users only wanting sums for each country
    //    (only display the 'all' key)?

    @GetMapping("/api/all")
    public Map<String, Map<String, Map<String, Map<String, Integer>>>> all(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state) {
        return query(date, country, state, null);
    }


    @GetMapping("/api/confirmed")
    public Map<String, Map<String, Map<String, Map<String, Integer>>>> confirmed(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state) {
        return query(date, country, state, CaseType.CONFIRMED);
    }

    @GetMapping("/api/deaths")
    public Map<String, Map<String, Map<String, Map<String, Integer>>>> deaths(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state) {
        return query(date, country, state, CaseType.DEATHS);
    }

    @GetMapping("api/recovered")
    public Map<String, Map<String, Map<String, Map<String, Integer>>>> recovered(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state) {
        return query(date, country, state, CaseType.RECOVERED);
    }
}
