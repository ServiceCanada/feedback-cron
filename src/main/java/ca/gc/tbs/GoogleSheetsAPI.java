package ca.gc.tbs;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Google Sheets API client for appending feedback data.
 * Uses modern GoogleCredentials with JSON key file and implements credential caching,
 * retry logic, and thread-safe operations.
 */
public class GoogleSheetsAPI {
    private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsAPI.class);

    // TODO: Externalize these to application.properties
    static final String SPREADSHEET_ID = "1B16qEbfp7SFCfIsZ8fcj7DneCy1WkR0GPh4t9L9NRSg";
    static final String DUPLICATE_COMMENTS_SPREADSHEET_ID = "1cR2mih5sBwl3wUjniwdyVA0xZcqV2Wl9yhghJfMG5oM";
    static final String URL_RANGE = "A1:A50000";
    static final String DUPLICATE_RANGE = "A1:D50000";

    private static final String APPLICATION_NAME = "Page Feedback CronJob";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String SERVICE_ACCOUNT_KEY_FILE = "service-account.json";

    // Retry configuration
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 1000;

    // Cached Sheets service instance (thread-safe lazy initialization)
    private static volatile Sheets sheetsService;
    private static final Object lock = new Object();

    /**
     * Gets or creates a cached Sheets service instance.
     * Thread-safe singleton pattern with double-checked locking.
     *
     * @return Sheets service instance
     * @throws IOException if service account key file cannot be read
     * @throws GeneralSecurityException if HTTP transport cannot be created
     */
    private static Sheets getSheetsService() throws IOException, GeneralSecurityException {
        if (sheetsService == null) {
            synchronized (lock) {
                if (sheetsService == null) {
                    logger.debug("Initializing Google Sheets service");
                    sheetsService = createSheetsService();
                }
            }
        }
        return sheetsService;
    }

    /**
     * Creates a new Sheets service instance with modern GoogleCredentials.
     *
     * @return configured Sheets service
     * @throws IOException if service account key file cannot be read
     * @throws GeneralSecurityException if HTTP transport cannot be created
     */
    private static Sheets createSheetsService() throws IOException, GeneralSecurityException {
        NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        GoogleCredentials credentials;
        try (InputStream keyStream = GoogleSheetsAPI.class.getClassLoader()
                .getResourceAsStream(SERVICE_ACCOUNT_KEY_FILE)) {

            if (keyStream == null) {
                throw new IOException("Service account key file not found: " + SERVICE_ACCOUNT_KEY_FILE);
            }

            credentials = GoogleCredentials.fromStream(keyStream)
                    .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS));
        }

        return new Sheets.Builder(httpTransport, JSON_FACTORY, new HttpCredentialsAdapter(credentials))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    /**
     * Appends a URL to the main feedback spreadsheet with retry logic.
     *
     * @param url the URL to append
     * @throws IOException if all retry attempts fail
     * @throws GeneralSecurityException if unable to create HTTP transport
     */
    public static void appendURL(String url) throws IOException, GeneralSecurityException {
        logger.debug("Appending URL to spreadsheet: {}", url);
        appendValues(SPREADSHEET_ID, URL_RANGE, Collections.singletonList(url));
    }

    /**
     * Appends duplicate comment data to the duplicate comments spreadsheet with retry logic.
     *
     * @param date the date of the comment
     * @param timestamp the timestamp of the comment
     * @param url the URL associated with the comment
     * @param comment the comment text
     * @throws IOException if all retry attempts fail
     * @throws GeneralSecurityException if unable to create HTTP transport
     */
    public static void appendDuplicateComment(String date, String timestamp, String url, String comment)
            throws IOException, GeneralSecurityException {
        logger.debug("Appending duplicate comment - Date: {}, URL: {}", date, url);
        appendValues(DUPLICATE_COMMENTS_SPREADSHEET_ID, DUPLICATE_RANGE,
                Arrays.asList(date, timestamp, url, comment));
    }

    /**
     * Generic method to append values to a spreadsheet with exponential backoff retry.
     *
     * @param spreadsheetId the ID of the target spreadsheet
     * @param range the A1 notation range
     * @param values the values to append
     * @throws IOException if all retry attempts fail
     * @throws GeneralSecurityException if unable to create HTTP transport
     */
    private static void appendValues(String spreadsheetId, String range, List<Object> values)
            throws IOException, GeneralSecurityException {

        ValueRange appendBody = new ValueRange()
                .setValues(Collections.singletonList(values));

        IOException lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                Sheets service = getSheetsService();
                AppendValuesResponse response = service.spreadsheets().values()
                        .append(spreadsheetId, range, appendBody)
                        .setValueInputOption("USER_ENTERED")
                        .setInsertDataOption("INSERT_ROWS")
                        .setIncludeValuesInResponse(false)
                        .execute();

                logger.debug("Successfully appended values to spreadsheet {} on attempt {}",
                        spreadsheetId, attempt);
                return; // Success

            } catch (IOException e) {
                lastException = e;
                logger.warn("Attempt {}/{} failed to append to spreadsheet {}: {}",
                        attempt, MAX_RETRY_ATTEMPTS, spreadsheetId, e.getMessage());

                if (attempt < MAX_RETRY_ATTEMPTS) {
                    long delayMs = INITIAL_RETRY_DELAY_MS * (long) Math.pow(2, attempt - 1);
                    logger.debug("Retrying in {} ms", delayMs);
                    try {
                        TimeUnit.MILLISECONDS.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Retry interrupted", ie);
                    }
                }
            }
        }

        // All retries failed
        logger.error("Failed to append values to spreadsheet {} after {} attempts",
                spreadsheetId, MAX_RETRY_ATTEMPTS, lastException);
        throw lastException;
    }

    /**
     * Clears the cached Sheets service. Useful for testing or forcing re-initialization.
     */
    static void clearCache() {
        synchronized (lock) {
            sheetsService = null;
            logger.debug("Cleared cached Sheets service");
        }
    }

    /**
     * Main method for testing.
     */
    public static void main(String[] args) {
        try {
            appendURL("test-url");
            logger.info("Test append successful");
        } catch (Exception e) {
            logger.error("Test append failed", e);
        }
    }
}
