package ca.gc.tbs;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Collections;

public class GoogleSheetsAPI {
    private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsAPI.class);

    static final String spreadsheetId = "1B16qEbfp7SFCfIsZ8fcj7DneCy1WkR0GPh4t9L9NRSg";
    static final String duplicateCommentsSpreadsheetId = "1cR2mih5sBwl3wUjniwdyVA0xZcqV2Wl9yhghJfMG5oM"; // Template ID to
    // be replaced
    static final String range = "A1:A50000";
    private static final String APPLICATION_NAME = "My Google Sheets Application";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String SERVICE_ACCOUNT_EMAIL = "cronjob@feedback-cj.iam.gserviceaccount.com";
    /**
     * Global instance of the HTTP transport.
     */
    private static NetHttpTransport HTTP_TRANSPORT;

    public static void appendURL(String url) throws GeneralSecurityException, IOException {
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        keystore.load(GoogleSheetsAPI.class.getClassLoader().getResourceAsStream("service-account.p12"),
                "notasecret".toCharArray());
        PrivateKey pk = (PrivateKey) keystore.getKey("privatekey", "notasecret".toCharArray());

        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();

        GoogleCredential credential = new GoogleCredential.Builder().setTransport(HTTP_TRANSPORT)
                .setJsonFactory(JSON_FACTORY)
                .setServiceAccountId(SERVICE_ACCOUNT_EMAIL)
                .setServiceAccountScopes(Collections.singleton(SheetsScopes.SPREADSHEETS))
                .setServiceAccountPrivateKey(pk)
                .build();

        Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();

        ValueRange appendBody = new ValueRange()
                .setValues(Arrays.asList(
                        Arrays.asList(url)));
        try {
            AppendValuesResponse appendResult = service.spreadsheets().values()
                    .append(spreadsheetId, range, appendBody)
                    .setValueInputOption("USER_ENTERED")
                    .setInsertDataOption("INSERT_ROWS")
                    .setIncludeValuesInResponse(true)
                    .execute();
        } catch (IOException e) {
            logger.error("Error appending URL to spreadsheet: {}", url, e);
        }
    }

    public static void appendDuplicateComment(String date, String timestamp, String url, String comment)
            throws GeneralSecurityException, IOException {
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        keystore.load(GoogleSheetsAPI.class.getClassLoader().getResourceAsStream("service-account.p12"),
                "notasecret".toCharArray());
        PrivateKey pk = (PrivateKey) keystore.getKey("privatekey", "notasecret".toCharArray());

        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();

        GoogleCredential credential = new GoogleCredential.Builder().setTransport(HTTP_TRANSPORT)
                .setJsonFactory(JSON_FACTORY)
                .setServiceAccountId(SERVICE_ACCOUNT_EMAIL)
                .setServiceAccountScopes(Collections.singleton(SheetsScopes.SPREADSHEETS))
                .setServiceAccountPrivateKey(pk)
                .build();

        Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();

        ValueRange appendBody = new ValueRange()
                .setValues(Arrays.asList(
                        Arrays.asList(date, timestamp, url, comment)));
        try {
            AppendValuesResponse appendResult = service.spreadsheets().values()
                    .append(duplicateCommentsSpreadsheetId, "A1:D50000", appendBody)
                    .setValueInputOption("USER_ENTERED")
                    .setInsertDataOption("INSERT_ROWS")
                    .setIncludeValuesInResponse(true)
                    .execute();
        } catch (IOException e) {
            logger.error("Error appending duplicate comment to spreadsheet - Date: {}, URL: {}", date, url, e);
        }
    }

    public static void main(String[] args) throws GeneralSecurityException, IOException {
        appendURL("test");
    }
}
