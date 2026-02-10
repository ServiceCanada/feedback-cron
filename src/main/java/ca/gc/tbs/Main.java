package ca.gc.tbs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.mongodb.datatables.DataTablesRepositoryFactoryBean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import ca.gc.tbs.service.AirtableSyncService;
import ca.gc.tbs.service.ProblemCleaningService;
import ca.gc.tbs.service.SpreadsheetService;
import ca.gc.tbs.service.TopTaskCleaningService;

/**
 * Main entry point for the feedback processing cron job.
 * Orchestrates data cleaning and Airtable synchronization.
 */
@SpringBootApplication(exclude = {
    SecurityAutoConfiguration.class,
    DataSourceAutoConfiguration.class,
    HibernateJpaAutoConfiguration.class,
    JpaRepositoriesAutoConfiguration.class
})
@ComponentScan(
    basePackages = {
        "ca.gc.tbs.service",
        "ca.gc.tbs.repository",
        "ca.gc.tbs.domain"
    },
    excludeFilters = @ComponentScan.Filter(
        type = FilterType.REGEX,
        pattern = "ca\\.gc\\.tbs\\.service\\.(EmailService|ErrorKeywordService|ProblemCacheService|ProblemDateService|UserService)"
    )
)
@EnableMongoRepositories(
    basePackages = "ca.gc.tbs.repository",
    repositoryFactoryBeanClass = DataTablesRepositoryFactoryBean.class
)
public class Main implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private final TopTaskCleaningService topTaskCleaningService;
    private final ProblemCleaningService problemCleaningService;
    private final SpreadsheetService spreadsheetService;
    private final AirtableSyncService airtableSyncService;

    @Autowired
    public Main(TopTaskCleaningService topTaskCleaningService,
                ProblemCleaningService problemCleaningService,
                SpreadsheetService spreadsheetService,
                AirtableSyncService airtableSyncService) {
        this.topTaskCleaningService = topTaskCleaningService;
        this.problemCleaningService = problemCleaningService;
        this.spreadsheetService = spreadsheetService;
        this.airtableSyncService = airtableSyncService;
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder(Main.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        airtableSyncService.initialize();

        logger.info("Cleaning TTS data (personal info + junk removal)");
        topTaskCleaningService.cleanTopTaskSurveys();

        logger.info("Cleaning Problem data (personal info removal)");
        problemCleaningService.cleanProblems();

        logger.info("Importing spreadsheets");
        spreadsheetService.importTiers();

        logger.info("Airtable & spreadsheet sync");
        airtableSyncService.syncProblemsToAirtable();

        logger.info("Mark as processed");
        airtableSyncService.completeProcessing();
    }
}
