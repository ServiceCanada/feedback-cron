package ca.gc.tbs.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.sybit.airtable.Airtable;
import com.sybit.airtable.Base;
import com.sybit.airtable.Table;

import ca.gc.tbs.domain.Problem;
import ca.gc.tbs.model.AirTableProblemEnhanced;
import ca.gc.tbs.repository.ProblemRepository;
import ca.gc.tbs.util.UrlUtils;

/**
 * Service for syncing Problem data to Airtable.
 */
@Service
public class AirtableSyncService {
    private static final Logger logger = LoggerFactory.getLogger(AirtableSyncService.class);

    private static final int MAX_SYNC_RECORDS = 150;

    private final ProblemRepository problemRepository;
    private final SpreadsheetService spreadsheetService;

    @Value("${airtable.key}")
    private String airtableKey;

    @Value("${airtable.tab}")
    private String problemAirtableTab;

    @Value("${airtable.base}")
    private String problemAirtableBase;

    private Base mainBase;
    private Table<AirTableProblemEnhanced> mainTable;

    @Autowired
    public AirtableSyncService(ProblemRepository problemRepository, 
                                SpreadsheetService spreadsheetService) {
        this.problemRepository = problemRepository;
        this.spreadsheetService = spreadsheetService;
    }

    /**
     * Initializes the Airtable connection.
     */
    public void initialize() throws Exception {
        logger.info("Connecting to Airtable base");
        Airtable airtable = new Airtable().configure(airtableKey);
        mainBase = airtable.base(problemAirtableBase);
        mainTable = mainBase.table(problemAirtableTab, AirTableProblemEnhanced.class);
    }

    /**
     * Syncs unprocessed problems to Airtable based on tier classification.
     */
    public void syncProblemsToAirtable() {
        List<Problem> problems = fetchUnprocessedProblems();
        List<Problem> toSave = new ArrayList<>();

        logger.info("Found {} records to be processed on Date: {}",
                problems.size(), LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

        int processedCount = 0;
        for (Problem problem : problems) {
            if (processedCount >= MAX_SYNC_RECORDS) {
                logger.info("Reached sync limit of {} records", MAX_SYNC_RECORDS);
                break;
            }

            SyncResult result = processProblem(problem);
            if (result == SyncResult.SAVE) {
                toSave.add(problem);
                processedCount++;
            }
        }

        // Batch database operations
        if (!toSave.isEmpty()) {
            problemRepository.saveAll(toSave);
            logger.info("Batch saved {} problems", toSave.size());
        }
    }

    /**
     * Marks all processed problems as complete.
     */
    public void completeProcessing() {
        List<Problem> problems = problemRepository.findByProcessed("false");
        problems.addAll(problemRepository.findByProcessed(null));

        for (Problem problem : problems) {
            try {
                if ("true".equals(problem.getPersonalInfoProcessed())
                        && "true".equals(problem.getAirTableSync()) 
                        && (problem.getProcessed() == null || "false".equals(problem.getProcessed()))) {
                    problem.setProcessedDate(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    problem.setProcessed("true");
                    problemRepository.save(problem);
                }
            } catch (Exception e) {
                logger.error("Could not mark completed - ID: {}", problem.getId(), e);
            }
        }
        logger.info("Finished processing - all records marked complete");
    }

    private enum SyncResult { SAVE, SKIP }

    private List<Problem> fetchUnprocessedProblems() {
        List<Problem> problems = problemRepository.findByAirTableSync(null);
        problems.addAll(problemRepository.findByAirTableSync("false"));
        return problems;
    }

    private SyncResult processProblem(Problem problem) {
        try {
            // Extract UTM values BEFORE removing query params
            String utmValues = UrlUtils.extractUtmValues(problem.getUrl());

            // Normalize URL for tier comparisons
            problem.setUrl(UrlUtils.removeQueryAndFragment(problem.getUrl().toLowerCase()));

            routeProblem(problem, utmValues);
            return SyncResult.SAVE;

        } catch (Exception e) {
            logger.error("Could not sync record: {} - URL: {}", problem.getId(), problem.getUrl(), e);
            return SyncResult.SKIP;
        }
    }

    private void routeProblem(Problem problem, String utmValues) throws Exception {
        String url = problem.getUrl();

        if (!spreadsheetService.isTier1Url(url) && !spreadsheetService.isTier2Url(url)) {
            spreadsheetService.addUrlToTier2(problem);
        } else if (spreadsheetService.isTier2Url(url)) {
            markAsProcessed(problem);
        } else {
            syncProblemToAirtable(problem, utmValues);
        }
    }

    private void syncProblemToAirtable(Problem problem, String utmValues) throws Exception {
        AirTableProblemEnhanced airProblem = createAirTableProblem(problem, utmValues);
        mainTable.create(airProblem);
        problem.setAirTableSync("true");
        logger.info("Synced to Airtable (Tier 1): {}", problem.getUrl());
    }

    private void markAsProcessed(Problem problem) {
        problem.setAirTableSync("true");
        logger.debug("Tier 2 URL already exists: {}", problem.getUrl());
    }

    private AirTableProblemEnhanced createAirTableProblem(Problem problem, String utmValues) {
        AirTableProblemEnhanced airProblem = new AirTableProblemEnhanced();
        airProblem.setUTM(utmValues);
        airProblem.setUniqueID(problem.getId());
        airProblem.setDate(problem.getProblemDate());
        airProblem.setTimeStamp(problem.getTimeStamp());
        airProblem.setURL(problem.getUrl());
        airProblem.setLang(problem.getLanguage().toUpperCase());
        airProblem.setComment(problem.getProblemDetails());
        airProblem.setIgnore(null);
        airProblem.setTagsConfirmed(null);
        airProblem.setRefiningDetails("");
        airProblem.setActionable(null);
        airProblem.setMainSection(problem.getSection());
        airProblem.setStatus("New");
        airProblem.setLookupTags(null);
        airProblem.setPageTitle(problem.getTitle());
        airProblem.setInstitution(problem.getInstitution());
        airProblem.setTheme(problem.getTheme());
        airProblem.setId(null);
        return airProblem;
    }
}
