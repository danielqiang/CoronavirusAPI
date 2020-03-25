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
import java.util.stream.Collectors;

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

    /**
     * Helper method to reformat a date from `MMddyyyy` format to `M/d/YY` format.
     * If the date is invalid, returns an empty string.
     */
    private String reformatDate(String date) {
        try {
            Date newDate = new SimpleDateFormat("MMddyyyy").parse(date);

            return new SimpleDateFormat("M/d/yy").format(newDate);
        } catch (ParseException e) {
            return "";
        }
    }

    /**
     * Helper method to filter internal `data` map via streams.
     *
     * @param caseType if null, matches all case types. Otherwise,
     *                 matches the provided case type.
     */
    private Map<String, Map<String, Map<String, Map<String, String>>>>
    filter(String date, String country, String state, CaseType caseType) {
        String formattedDate = (date.equals("all")) ? "all" : reformatDate(date);
        String caseName = (caseType == null) ? null : caseType.name().toLowerCase();

        return data.entrySet().stream()
                .filter(e -> (e.getKey().equalsIgnoreCase(country)
                        || country.equals("all")))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().entrySet().stream()
                                .filter(e1 -> (e1.getKey().equalsIgnoreCase(state)
                                        || state.equals("all")))
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e1 -> e1.getValue().entrySet().stream()
                                                .filter(e2 -> (e2.getKey().equals(formattedDate)
                                                        || formattedDate.equals("all")))
                                                .collect(Collectors.toMap(
                                                        Map.Entry::getKey,
                                                        e2 -> e2.getValue().entrySet().stream()
                                                                .filter(e3 -> e3.getKey().equals(caseName)
                                                                        || caseName == null)
                                                                .collect(Collectors.toMap(
                                                                        Map.Entry::getKey,
                                                                        Map.Entry::getValue
                                                                        )
                                                                )
                                                        )
                                                ).entrySet().stream()
                                                .filter(e2 -> (!e2.getValue().isEmpty()))
                                                .collect(Collectors.toMap(
                                                        Map.Entry::getKey,
                                                        Map.Entry::getValue
                                                        )
                                                )
                                        )
                                ).entrySet().stream()
                                .filter(e1 -> (!e1.getValue().isEmpty()))
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue
                                        )
                                )
                        )
                ).entrySet().stream()
                .filter(e -> (!e.getValue().isEmpty()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                        )
                );
    }

    @GetMapping("/api/all")
    public Map<String, Map<String, Map<String, Map<String, String>>>> all(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state
    ) {
        return filter(date, country, state, null);
    }


    @GetMapping("/api/confirmed")
    public Map<String, Map<String, Map<String, Map<String, String>>>> confirmed(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state
    ) {
        return filter(date, country, state, CaseType.CONFIRMED);
    }

    @GetMapping("/api/deaths")
    public Map<String, Map<String, Map<String, Map<String, String>>>> deaths(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state
    ) {
        return filter(date, country, state, CaseType.DEATHS);
    }

    @GetMapping("api/recovered")
    public Map<String, Map<String, Map<String, Map<String, String>>>> recovered(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state
    ) {
        return filter(date, country, state, CaseType.RECOVERED);
    }
}
