package ca.gc.tbs.util;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;

/**
 * Utility class for content validation operations.
 */
public final class ValidationUtils {

    private ValidationUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Checks if the given text contains HTML markup.
     *
     * @param text the text to check
     * @return true if HTML is detected, false otherwise
     */
    public static boolean containsHTML(String text) {
        if (text == null) return false;
        text = StringUtils.normalizeSpace(text);
        String parsedText = Jsoup.parse(text).text().trim();
        return parsedText.length() != text.trim().length();
    }

    /**
     * Checks if a normalized comment already exists in the seen comments set.
     *
     * @param normalizedComment the comment normalized to lowercase and trimmed
     * @param seenComments set of previously seen comments
     * @return true if duplicate, false otherwise
     */
    public static boolean isDuplicateComment(String normalizedComment, Set<String> seenComments) {
        return seenComments.contains(normalizedComment);
    }
}
