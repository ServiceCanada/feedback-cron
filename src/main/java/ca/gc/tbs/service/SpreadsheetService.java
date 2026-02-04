package ca.gc.tbs.service;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import ca.gc.tbs.GoogleSheetsAPI;
import ca.gc.tbs.domain.Problem;

/**
 * Service for managing tier spreadsheet operations.
 */
@Service
public class SpreadsheetService {
    private static final Logger logger = LoggerFactory.getLogger(SpreadsheetService.class);

    private static final String TIER1_SPREADSHEET_URL = 
            "https://docs.google.com/spreadsheets/d/1eOmX_b8XCR9eLNxUbX3Gwkp2ywJ-vhapnC7ApdRbnSg/export?format=csv";
    private static final String TIER2_SPREADSHEET_URL = 
            "https://docs.google.com/spreadsheets/d/1B16qEbfp7SFCfIsZ8fcj7DneCy1WkR0GPh4t9L9NRSg/export?format=csv";

    private final Set<String> tier1Urls = new HashSet<>();
    private final Set<String> tier2Urls = new HashSet<>();

    /**
     * Imports both Tier 1 and Tier 2 spreadsheets.
     */
    public void importTiers() throws Exception {
        importTier1();
        importTier2();
    }

    /**
     * Imports Tier 1 URLs from the spreadsheet.
     */
    public void importTier1() throws Exception {
        parseCsvFromUrl(TIER1_SPREADSHEET_URL, record -> {
            tier1Urls.add(record.get("URL").toLowerCase());
        }, "Tier 1");
        logger.info("Imported {} Tier 1 URLs", tier1Urls.size());
    }

    /**
     * Imports Tier 2 URLs from the spreadsheet.
     */
    public void importTier2() throws Exception {
        parseCsvFromUrl(TIER2_SPREADSHEET_URL, record -> {
            tier2Urls.add(record.get("URL").toLowerCase());
        }, "Tier 2");
        logger.info("Imported {} Tier 2 URLs", tier2Urls.size());
    }

    /**
     * Checks if a URL is in the Tier 1 spreadsheet.
     */
    public boolean isTier1Url(String url) {
        return tier1Urls.contains(url);
    }

    /**
     * Checks if a URL is in the Tier 2 spreadsheet.
     */
    public boolean isTier2Url(String url) {
        return tier2Urls.contains(url);
    }

    /**
     * Logs a duplicate comment to the Google Sheets duplicate tracker.
     */
    public void logDuplicateComment(Problem problem) {
        try {
            String date = problem.getProblemDate() != null
                    ? problem.getProblemDate()
                    : LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            GoogleSheetsAPI.appendDuplicateComment(date, problem.getTimeStamp(), 
                    problem.getUrl(), problem.getProblemDetails());
        } catch (Exception e) {
            logger.error("Error writing duplicate to spreadsheet", e);
        }
    }

    /**
     * Adds a URL to the Tier 2 spreadsheet (both local cache and Google Sheets).
     */
    public void addUrlToTier2(Problem problem) throws Exception {
        tier2Urls.add(problem.getUrl());
        GoogleSheetsAPI.appendURL(problem.getUrl());
        problem.setAirTableSync("true");
        logger.info("URL not in spreadsheet: {}, added to Tier 2 Spreadsheet", problem.getUrl());
    }

    private void parseCsvFromUrl(String url, Consumer<CSVRecord> recordProcessor, String tierName) 
            throws Exception {
        try (Reader reader = new InputStreamReader(
                new URL(url).openConnection().getInputStream(),
                StandardCharsets.UTF_8)) {

            final CSVFormat csvFormat = CSVFormat.Builder.create()
                    .setHeader()
                    .setAllowMissingColumnNames(true)
                    .build();
            final Iterable<CSVRecord> records = csvFormat.parse(reader);

            for (final CSVRecord record : records) {
                try {
                    recordProcessor.accept(record);
                } catch (Exception e) {
                    logger.error("Error importing {} spreadsheet record", tierName, e);
                }
            }
        }
    }
}
