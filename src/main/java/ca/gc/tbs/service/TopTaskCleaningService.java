package ca.gc.tbs.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ca.gc.tbs.domain.TopTaskSurvey;
import ca.gc.tbs.repository.TopTaskRepository;
import ca.gc.tbs.util.ValidationUtils;

/**
 * Service for cleaning Top Task Survey data.
 */
@Service
public class TopTaskCleaningService {
    private static final Logger logger = LoggerFactory.getLogger(TopTaskCleaningService.class);

    private final ContentService contentService;
    private final TopTaskRepository topTaskRepository;

    @Autowired
    public TopTaskCleaningService(ContentService contentService, TopTaskRepository topTaskRepository) {
        this.contentService = contentService;
        this.topTaskRepository = topTaskRepository;
    }

    /**
     * Cleans all unprocessed Top Task Survey entries.
     * Removes junk entries and cleans personal info from valid records.
     */
    public void cleanTopTaskSurveys() {
        List<TopTaskSurvey> tasks = topTaskRepository.findByProcessed("false");
        logger.info("Number of TTS entries to clean: {}", tasks.size());

        for (TopTaskSurvey task : tasks) {
            try {
                processTask(task);
            } catch (Exception e) {
                logger.error("Could not process task: {} - DateTime: {}", 
                        task.getId(), task.getDateTime(), e);
            }
        }
        logger.info("TTS cleaning complete");
    }

    private void processTask(TopTaskSurvey task) {
        // Check for junk first - delete immediately without wasting time cleaning
        if (task == null || hasHTMLInAnyField(task)) {
            assert task != null;
            logger.warn("Deleting junk task: {} - Had null or hyperlink", task.getId());
            topTaskRepository.delete(task);
            return;
        }

        // Trim whitespace
        trimWhitespaceField(task.getTaskOther(), task::setTaskOther, "taskOther");
        trimWhitespaceField(task.getThemeOther(), task::setThemeOther, "themeOther");
        trimWhitespaceField(task.getTaskImproveComment(), task::setTaskImproveComment, "taskImproveComment");
        trimWhitespaceField(task.getTaskWhyNotComment(), task::setTaskWhyNotComment, "taskWhyNotComment");

        // Clean personal info from valid records
        cleanTaskField(task.getThemeOther(), task::setThemeOther);
        cleanTaskField(task.getTaskOther(), task::setTaskOther);
        cleanTaskField(task.getTaskImproveComment(), task::setTaskImproveComment);
        cleanTaskField(task.getTaskWhyNotComment(), task::setTaskWhyNotComment);

        task.setPersonalInfoProcessed("true");
        task.setProcessed("true");
        task.setProcessedDate(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        topTaskRepository.save(task);
    }

    private void cleanTaskField(String fieldValue, Consumer<String> setter) {
        if (fieldValue != null) {
            String cleaned = contentService.cleanContent(fieldValue);
            setter.accept(cleaned);
        }
    }

    private boolean hasHTMLInAnyField(TopTaskSurvey task) {
        return ValidationUtils.containsHTML(task.getTaskOther())
                || ValidationUtils.containsHTML(task.getThemeOther())
                || ValidationUtils.containsHTML(task.getTaskImproveComment())
                || ValidationUtils.containsHTML(task.getTaskWhyNotComment());
    }

    private void trimWhitespaceField(String fieldValue, Consumer<String> setter, String fieldName) {
        if (fieldValue != null && fieldValue.trim().isEmpty() && !fieldValue.isEmpty()) {
            logger.debug("Found junk data in {}", fieldName);
            setter.accept("");
        }
    }
}
