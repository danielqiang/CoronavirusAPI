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
    // TODO: Note to Anjali. I changed the map signature so that
    //  the number of cases for each case type (confirmed, deaths, recovered)
    //  is represented as an integer.
    private final Map<String, Map<String, Map<String, Map<String, Integer>>>> data;

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
            Map<String, Map<String, Map<String, Map<String, Integer>>>> out,
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
                int cases = Integer.parseInt(row.get(date));
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
     * Helper method to query internal `data` map via streams.
     *
     * @param caseType if null, matches all case types. Otherwise,
     *                 matches the provided case type.
     */
    private Map<String, Map<String, Map<String, Map<String, Integer>>>>
    query(String date, String country, String state, CaseType caseType) {
        String formattedDate = (date.equals("all")) ? "all" : reformatDate(date);
        String caseName = (caseType == null) ? null : caseType.name().toLowerCase();

        // `data` map layout:
        //     {
        //        country: {
        //            state: {
        //                date: {
        //                    "confirmed": x,
        //                    "deaths": y,
        //                    "recovered": z
        //                }, ...
        //            }, ...
        //        }, ...
        //    }
        //
        // If `date` is "all", then all dates are matched.
        // If `state` is "all", then all states are matched.
        // If `country` is "all", then all countries are matched.
        //
        // TODO: Note to Anjali. This method looks hefty because it's basically trying
        //  to use `data` as a database. When we start using an actual database, this
        //  code will likely be deprecated since SQL does all of this for us
        //  and much more.
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
                                                                // "recovered", values are integers.
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
                // This can happen if `state` is not found in `data`
                // and all countries are matched.
                .filter(e -> (!e.getValue().isEmpty()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                        )
                );
    }

    @GetMapping("/api/all")
    public Map<String, Map<String, Map<String, Map<String, Integer>>>> all(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state
    ) {
        return query(date, country, state, null);
    }


    @GetMapping("/api/confirmed")
    public Map<String, Map<String, Map<String, Map<String, Integer>>>> confirmed(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state
    ) {
        return query(date, country, state, CaseType.CONFIRMED);
    }

    @GetMapping("/api/deaths")
    public Map<String, Map<String, Map<String, Map<String, Integer>>>> deaths(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state
    ) {
        return query(date, country, state, CaseType.DEATHS);
    }

    @GetMapping("api/recovered")
    public Map<String, Map<String, Map<String, Map<String, Integer>>>> recovered(
            @RequestParam(value = "date", defaultValue = "all") String date,
            @RequestParam(value = "country", defaultValue = "all") String country,
            @RequestParam(value = "state", defaultValue = "all") String state
    ) {
        return query(date, country, state, CaseType.RECOVERED);
    }
}
