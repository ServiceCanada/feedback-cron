package ca.gc.tbs;

import ca.gc.tbs.domain.Problem;
import ca.gc.tbs.domain.TopTaskSurvey;
import ca.gc.tbs.repository.ProblemRepository;
import ca.gc.tbs.repository.TopTaskRepository;
import ca.gc.tbs.service.ContentService;
import com.sybit.airtable.Airtable;
import com.sybit.airtable.Base;
import com.sybit.airtable.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.datatables.DataTablesRepositoryFactoryBean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.System.exit;

@SpringBootApplication
@ComponentScan(basePackages = {"ca.gc.tbs.domain", "ca.gc.tbs.repository", "ca.gc.tbs.service"})
@EnableMongoRepositories(repositoryFactoryBeanClass = DataTablesRepositoryFactoryBean.class)
public class Main implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    
    // Spreadsheet URLs
    private static final String TIER1_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1eOmX_b8XCR9eLNxUbX3Gwkp2ywJ-vhapnC7ApdRbnSg/export?format=csv";
    private static final String TIER2_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1B16qEbfp7SFCfIsZ8fcj7DneCy1WkR0GPh4t9L9NRSg/export?format=csv";
    
    // Tier 2 entries do not populate to AirTable.
    private final Set<String> tier2Spreadsheet = new HashSet<>();
    private final HashMap<String, String[]> tier1Spreadsheet = new HashMap<>();
    
    @Autowired
    private ContentService contentService;
    
    @Autowired
    private ProblemRepository problemRepository;
    @Autowired
    private TopTaskRepository topTaskRepository;
    // Main AirTable
    @Value("${airtable.key}")
    private String airtableKey;
    @Value("${airtable.tab}")
    private String problemAirtableTab;
    @Value("${airtable.base}")
    private String problemAirtableBase;

    // Health AirTable
    @Value("${health.airtable.base}")
    private String healthAirtableBase;

    // CRA AirTable
    @Value("${cra.airtable.base}")
    private String CRA_AirtableBase;

    // Travel AirTable
    @Value("${travel.airtable.base}")
    private String travelAirtableBase;

    // IRCC AirTable
    @Value("${ircc.airtable.base}")
    private String irccAirtableBase;

    private Base mainBase;
    private Base healthBase;
    private Base CRA_Base;
    private Base travelBase;
    private Base IRCC_Base;

    public static void main(String[] args) {
        new SpringApplicationBuilder(Main.class).web(WebApplicationType.NONE) // .REACTIVE, .SERVLET
                .run(args);
    }

    // Main Loop, Runs all functions needed.
    @Override
    public void run(String... args) throws Exception {

        Airtable airTableKey = new Airtable().configure(this.airtableKey);

        logger.info("Connecting to Airtable bases");
        this.mainBase = airTableKey.base(this.problemAirtableBase);
        this.healthBase = airTableKey.base(this.healthAirtableBase);
        this.CRA_Base = airTableKey.base(this.CRA_AirtableBase);
        this.travelBase = airTableKey.base(this.travelAirtableBase);
        this.IRCC_Base = airTableKey.base(this.irccAirtableBase);

        logger.info("Removing personal info from TTS");
        this.removePersonalInfoExitSurvey();

        logger.info("Removing personal info from comments");
        this.removePersonalInfoProblems();

        logger.info("Removing junk data from TTS");
        this.removeJunkDataTTS();

        logger.info("Importing spreadsheets");
        this.importTier1();
        this.importTier2();

        logger.info("Airtable & spreadsheet sync");
        this.airTableSpreadsheetSync();

        logger.info("Mark as processed");
        this.completeProcessing();
    }

    // Scrubs tasks (Exit Survey) that have not been cleaned using the cleaning script
    public void removePersonalInfoExitSurvey() {
        List<TopTaskSurvey> tList = this.topTaskRepository.findByPersonalInfoProcessed(null);
        tList.addAll(this.topTaskRepository.findByPersonalInfoProcessed("false"));
        logger.info("Number of tasks to clean: {}", tList.size());
        for (TopTaskSurvey task : tList) {
            try {
                if (task.getThemeOther() != null) {
                    String details = this.contentService.cleanContent(task.getThemeOther());
                    task.setThemeOther(details);
                }
                if (task.getTaskOther() != null) {
                    String details = this.contentService.cleanContent(task.getTaskOther());
                    task.setTaskOther(details);
                }
                if (task.getTaskImproveComment() != null) {
                    String details = this.contentService.cleanContent(task.getTaskImproveComment());
                    task.setTaskImproveComment(details);
                }
                if (task.getTaskWhyNotComment() != null) {
                    String details = this.contentService.cleanContent(task.getTaskWhyNotComment());
                    task.setTaskWhyNotComment(details);
                }
                task.setPersonalInfoProcessed("true");
                this.topTaskRepository.save(task);
            } catch (Exception e) {
                logger.error("Could not process task: {} - DateTime: {} - TaskOther: {} - ImproveComment: {} - WhyNotComment: {}",
                        task.getId(), task.getDateTime(), task.getTaskOther(), task.getTaskImproveComment(), task.getTaskWhyNotComment(), e);
            }
        }
        logger.info("Private info removed");
    }

    // Scrubs comments that have not been cleaned using the cleaning script
    public void removePersonalInfoProblems() {
        List<Problem> pList = this.problemRepository.findByPersonalInfoProcessed(null);
        pList.addAll(this.problemRepository.findByPersonalInfoProcessed("false"));
        logger.info("Number of Problems to clean: {}", pList.size());
        for (Problem problem : pList) {
            try {
                String details = this.contentService.cleanContent(problem.getProblemDetails());
                problem.setProblemDetails(details);
                problem.setPersonalInfoProcessed("true");
                this.problemRepository.save(problem);
            } catch (Exception e) {
                logger.error("Could not process problem: {} - Details: {}", problem.getId(), problem.getProblemDetails(), e);
            }
        }
        logger.info("Private info removed");
    }

    // Removes white space values from comments to improve the filter for write in comments on the Feedback-Viewer.
    public void removeJunkDataTTS() {
        List<TopTaskSurvey> tList = this.topTaskRepository.findByProcessed("false");
        logger.info("Amount of non processed entries (TTS): {}", tList.size());
        for (TopTaskSurvey task : tList) {
            if (task == null || containsHTML(task.getTaskOther()) || containsHTML(task.getThemeOther()) ||
                    containsHTML(task.getTaskImproveComment()) || containsHTML(task.getTaskWhyNotComment())) {
                assert task != null;
                logger.warn("Deleting task: {} - Task was null or had a hyperlink - TaskOther: {}, ThemeOther: {}, WhyNotComment: {}, ImproveComment: {}",
                        task.getId(), task.getTaskOther(), task.getThemeOther(), task.getTaskWhyNotComment(), task.getTaskImproveComment());
                this.topTaskRepository.delete(task);
                continue;
            }
            if (task.getTaskOther() != null && task.getTaskOther().trim().equals("") && task.getTaskOther().length() != 0) {
                logger.debug("Found junk data in taskOther");
                task.setTaskOther("");
            }
            if (task.getThemeOther() != null && task.getThemeOther().trim().equals("") && task.getThemeOther().length() != 0) {
                logger.debug("Found junk data in themeOther");
                task.setThemeOther("");
            }
            if (task.getTaskImproveComment() != null && task.getTaskImproveComment().trim().equals("") && task.getTaskImproveComment().length() != 0) {
                logger.debug("Found junk data in taskImproveComment");
                task.setTaskImproveComment("");
            }
            if (task.getTaskWhyNotComment() != null && task.getTaskWhyNotComment().trim().equals("") && task.getTaskWhyNotComment().length() != 0) {
                logger.debug("Found junk data in taskWhyNotComment");
                task.setTaskWhyNotComment("");
            }
            task.setProcessed("true");
            task.setProcessedDate(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
            this.topTaskRepository.save(task);
        }
    }

    // Retrieves ALL model & bases from spreadsheet and imports them to the TIER 1 map.
    public void importTier1() throws Exception {
        parseCsvFromUrl(TIER1_SPREADSHEET_URL, record -> {
            String[] modelBase = {record.get("MODEL"), record.get("BASE").toLowerCase()};
            tier1Spreadsheet.put(record.get("URL").toLowerCase(), modelBase);
        }, "Tier 1");
    }

    // Retrieves ALL URLs from spreadsheet and imports them to the TIER 2 map
    public void importTier2() throws Exception {
        parseCsvFromUrl(TIER2_SPREADSHEET_URL, record -> {
            tier2Spreadsheet.add(record.get("URL").toLowerCase());
        }, "Tier 2");
    }

    // Helper method to parse CSV from URL and process each record
    private void parseCsvFromUrl(String url, java.util.function.Consumer<CSVRecord> recordProcessor, String tierName) throws Exception {
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




    // Populates entries to the AirTable bases and Tier 2 spreadsheet (inventory).
    private void writeDuplicateToFile(String comment, String url, String date, String timeStamp) {
        try {
            GoogleSheetsAPI.appendDuplicateComment(date, timeStamp, url, comment);
        } catch (Exception e) {
            logger.error("Error writing duplicate to spreadsheet", e);
        }
    }


    // Populates entries to the AirTable bases and Tier 2 spreadsheet (inventory).
    @SuppressWarnings("unchecked")
    public void airTableSpreadsheetSync() {
        // Connect to AirTable bases
        Table<AirTableProblemEnhanced> problemTable = mainBase.table(this.problemAirtableTab, AirTableProblemEnhanced.class);
        Table<AirTableProblemEnhanced> healthTable = healthBase.table(this.problemAirtableTab, AirTableProblemEnhanced.class);
        Table<AirTableProblemEnhanced> craTable = CRA_Base.table(this.problemAirtableTab, AirTableProblemEnhanced.class);
        Table<AirTableProblemEnhanced> travelTable = travelBase.table(this.problemAirtableTab, AirTableProblemEnhanced.class);
        Table<AirTableProblemEnhanced> irccTable = IRCC_Base.table(this.problemAirtableTab, AirTableProblemEnhanced.class);
        // Find problems that have not been run through this function
        Set<String> seenComments = new HashSet<>();
        List<Problem> pList = this.problemRepository.findByAirTableSync(null);
        pList.addAll(this.problemRepository.findByAirTableSync("false"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        logger.info("Connected to MongoDB & Airtable");
        logger.info("Found {} records to be processed on Date: {}", pList.size(), LocalDate.now().format(formatter));
        int i = 1;
        int maxToSync = 150;
        for (Problem problem : pList) {
            try {
                if (i >= maxToSync) {
                    logger.info("Sync only {} records at a time", maxToSync);
                    break;
                }
                // In airTableSpreadsheetSync(), right after getting the pList:

                // Then in the for loop, before processing each problem:
                String normalizedComment = problem.getProblemDetails().trim().toLowerCase();

                if (seenComments.contains(normalizedComment)) {
                    logger.info("Skipping duplicate comment: {}", problem.getProblemDetails());
                    writeDuplicateToFile(problem.getProblemDetails(), problem.getUrl(),
                            problem.getProblemDate() != null ? problem.getProblemDate() : LocalDate.now().format(formatter), problem.getTimeStamp());
                    problem.setAirTableSync("true"); // Mark as processed
                    problemRepository.save(problem);
                    continue;
                }
                seenComments.add(normalizedComment);


                boolean problemIsProcessed = "true".equals(problem.getPersonalInfoProcessed());
                boolean junkComment = problem.getProblemDetails().trim().isEmpty() || containsHTML(problem.getProblemDetails())
                        || problem.getUrl().equals("https://www.canada.ca/") || problem.getProblemDetails().length() > 301;
                if (junkComment) {
                    logger.info("Empty comment, deleting entry");
                    problemRepository.delete(problem);
                    continue;
                }
                String UTM_values = extractUtmValues(problem.getUrl());
                problem.setUrl(removeQueryAndFragment(problem.getUrl().toLowerCase()));

                // if tier 1 and tier 2 spreadsheet don't contain URL, add it to Tier 2 and set sync to true
                if (!tier1Spreadsheet.containsKey(problem.getUrl()) && !tier2Spreadsheet.contains(problem.getUrl())) {
                    tier2Spreadsheet.add(problem.getUrl());
                    GoogleSheetsAPI.appendURL(problem.getUrl());
                    problem.setAirTableSync("true");
                    logger.info("Processed record: {} - URL not in spreadsheet: {}, added to Tier 2 Spreadsheet", i, problem.getUrl());
                }
                // if tier 2 spreadsheet contains URL set AirTable sync to true // TIER 2 entries end here.
                else if (tier2Spreadsheet.contains(problem.getUrl())) {
                    problem.setAirTableSync("true");
                    logger.debug("Processed record: {} (Tier 2) already exists", i);
                } else {
                    AirTableProblemEnhanced airProblem = new AirTableProblemEnhanced();
                    String base = tier1Spreadsheet.get(problem.getUrl())[1];

                    airProblem.setUTM(UTM_values);
                    setAirProblemAttributes(airProblem, problem);

                    switch (base.toLowerCase()) {
                        case "main":
                            problemTable.create(airProblem);
                            break;
                        case "ircc":
                            irccTable.create(airProblem);
                            break;
                        case "travel":
                            travelTable.create(airProblem);
                            break;
                        case "cra":
                            craTable.create(airProblem);
                            break;
                        case "health":
                            healthTable.create(airProblem);
                            break;
                    }
                    problem.setAirTableSync("true");
                    logger.info("Processed record: {} (Tier 1) Base: {}", i, base.toUpperCase());
                }
                i++;
                this.problemRepository.save(problem);
            } catch (Exception e) {
                logger.error("Could not sync record: {} - URL: {}", problem.getId(), problem.getUrl(), e);
            }
        }
    }

    // Marks problems as processed if applicable
    public void completeProcessing() {
        List<Problem> pList = this.problemRepository.findByProcessed("false");
        pList.addAll(this.problemRepository.findByProcessed(null));
        for (Problem problem : pList) {
            try {
                if ("true".equals(problem.getPersonalInfoProcessed())
                        && "true".equals(problem.getAirTableSync()) && (problem.getProcessed() == null || "false".equals(problem.getProcessed()))) {
                    problem.setProcessedDate(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    problem.setProcessed("true");
                    this.problemRepository.save(problem);
                }
            } catch (Exception e) {
                logger.error("Could not mark completed - ID: {}", problem.getId(), e);
            }
        }
        logger.info("Finished processing");
        exit(0);
    }

    public Boolean containsHTML(String comment) {
        if (comment == null) return false;
        // This normalizeSpace call was added because sometimes sentences are written with extra spaces between words which triggers as HTML.
        comment = StringUtils.normalizeSpace(comment);
        String parsedComment = Jsoup.parse(comment).text().trim();
        return parsedComment.length() != comment.trim().length();
    }

    public String extractUtmValues(String url) throws URISyntaxException {
        if (url == null) {
            return "";
        }

        try {
            new URL(url).toURI(); // check if the URL is well-formed
        } catch (MalformedURLException | URISyntaxException e) {
            return "";
        }

        URIBuilder builder = new URIBuilder(url);
        return builder.getQueryParams()
                .stream()
                .filter(x -> x.getName().startsWith("utm_"))
                .map(x -> x.getName() + "=" + x.getValue())
                .collect(Collectors.joining("&"));
    }


    public String removeQueryAndFragment(String url) {
        try {
            URIBuilder builder = new URIBuilder(url);
            // Remove query and fragment
            builder.clearParameters();
            builder.setFragment(null);
            return builder.build().toString();
        } catch (Exception e) {
            logger.error("Error removing query and fragment from URL: {}", url, e);
            return url; // Return the original URL if there's an exception
        }
    }


    // Sets attributes. Made it into a function to make the code look a bit more readable.
    public void setAirProblemAttributes(AirTableProblemEnhanced airProblem, Problem problem) {
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
        airProblem.setInstitution(problem.getInstitution());
        airProblem.setTheme(problem.getTheme());
        airProblem.setId(null);
    }




    public Base selectBase(String base) {
        if (base.equalsIgnoreCase("main")) {
            return mainBase;
        }
        if (base.equalsIgnoreCase("health")) {
            return healthBase;
        }
        if (base.equalsIgnoreCase("cra")) {
            return CRA_Base;
        }
        if (base.equalsIgnoreCase("ircc")) {
            return IRCC_Base;
        }
        if (base.equalsIgnoreCase("travel")) {
            return travelBase;
        }
        return null;
    }



}
