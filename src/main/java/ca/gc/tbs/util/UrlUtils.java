package ca.gc.tbs.util;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.stream.Collectors;

import org.apache.hc.core5.net.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for URL manipulation operations.
 */
public final class UrlUtils {
    private static final Logger logger = LoggerFactory.getLogger(UrlUtils.class);

    private UrlUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Extracts UTM parameters from a URL and returns them as a query string.
     *
     * @param url the URL to extract UTM values from
     * @return UTM parameters as "utm_x=value&utm_y=value" or empty string if none found
     */
    public static String extractUtmValues(String url) {
        if (url == null) {
            return "";
        }

        try {
            new URL(url).toURI();
        } catch (MalformedURLException | URISyntaxException e) {
            return "";
        }

        try {
            URIBuilder builder = new URIBuilder(url);
            return builder.getQueryParams()
                    .stream()
                    .filter(x -> x.getName().startsWith("utm_"))
                    .map(x -> x.getName() + "=" + x.getValue())
                    .collect(Collectors.joining("&"));
        } catch (URISyntaxException e) {
            logger.error("Error extracting UTM values from URL: {}", url, e);
            return "";
        }
    }

    /**
     * Removes query parameters and fragment from a URL.
     *
     * @param url the URL to clean
     * @return URL without query parameters and fragment, or original URL if parsing fails
     */
    public static String removeQueryAndFragment(String url) {
        try {
            URIBuilder builder = new URIBuilder(url);
            builder.clearParameters();
            builder.setFragment(null);
            return builder.build().toString();
        } catch (Exception e) {
            logger.error("Error removing query and fragment from URL: {}", url, e);
            return url;
        }
    }
}
