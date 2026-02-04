package ca.gc.tbs.service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ca.gc.tbs.domain.Problem;
import ca.gc.tbs.repository.ProblemRepository;
import ca.gc.tbs.util.ValidationUtils;
import ca.gc.tbs.service.BadWords;

/**
 * Service for cleaning Problem data.
 */
@Service
public class ProblemCleaningService {
    private static final Logger logger = LoggerFactory.getLogger(ProblemCleaningService.class);

    private static final int MAX_COMMENT_LENGTH = 301;

    private final ContentService contentService;
    private final ProblemRepository problemRepository;
    private final SpreadsheetService spreadsheetService;

    @Autowired
    public ProblemCleaningService(ContentService contentService,
                                   ProblemRepository problemRepository,
                                   SpreadsheetService spreadsheetService,
                                   BadWords badWords) {
        // Load BadWords config (JAR uses javax.annotation.PostConstruct which Spring Boot 3.x ignores)
        badWords.loadConfigs();

        this.contentService = contentService;
        this.problemRepository = problemRepository;
        this.spreadsheetService = spreadsheetService;
    }

    /**
     * Cleans all unprocessed Problem entries.
     * Removes junk/duplicates and cleans personal info from valid records.
     */
    public void cleanProblems() {
        Set<String> seenComments = new HashSet<>();
        List<Problem> problems = fetchUncleanedProblems();
        logger.info("Number of Problems to clean: {}", problems.size());

        for (Problem problem : problems) {
            try {
                processProblem(problem, seenComments);
            } catch (Exception e) {
                logger.error("Could not process problem: {} - Details: {}", 
                        problem.getId(), problem.getProblemDetails(), e);
            }
        }
        logger.info("Problem cleaning complete");
    }

    private List<Problem> fetchUncleanedProblems() {
        List<Problem> problems = problemRepository.findByPersonalInfoProcessed(null);
        problems.addAll(problemRepository.findByPersonalInfoProcessed("false"));
        return problems;
    }

    private void processProblem(Problem problem, Set<String> seenComments) {
        // Check for junk first - delete immediately without wasting time cleaning
        if (isJunkComment(problem)) {
            logger.info("Deleting junk comment: {}", problem.getId());
            problemRepository.delete(problem);
            return;
        }

        // Check for duplicates within this batch
        String normalizedComment = problem.getProblemDetails().trim().toLowerCase();
        if (ValidationUtils.isDuplicateComment(normalizedComment, seenComments)) {
            logger.info("Deleting duplicate comment: {}", problem.getProblemDetails());
            spreadsheetService.logDuplicateComment(problem);
            problemRepository.delete(problem);
            return;
        }
        seenComments.add(normalizedComment);

        // Clean personal info from valid, non-duplicate records
        String details = contentService.cleanContent(problem.getProblemDetails());
        problem.setProblemDetails(details);
        problem.setPersonalInfoProcessed("true");
        problemRepository.save(problem);
    }

    private boolean isJunkComment(Problem problem) {
        String details = problem.getProblemDetails();
        String url = problem.getUrl();
        return details.trim().isEmpty()
                || ValidationUtils.containsHTML(details)
                || "https://www.canada.ca/".equals(url)
                || details.length() > MAX_COMMENT_LENGTH;
    }
}
