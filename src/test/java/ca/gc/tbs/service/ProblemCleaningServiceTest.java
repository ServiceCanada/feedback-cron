package ca.gc.tbs.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import ca.gc.tbs.domain.Problem;
import ca.gc.tbs.repository.ProblemRepository;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

class ProblemCleaningServiceTest {

    private static final String RAW = "My name is John Smith, call me at 613-555-0199";
    private static final String CLEANED = "My name is ##########, call me at ###-###-####";

    private ContentService contentService;
    private ProblemRepository problemRepository;
    private SpreadsheetService spreadsheetService;
    private ProblemCleaningService service;
    private Logger logger;
    private ListAppender<ILoggingEvent> logs;

    @BeforeEach
    void setUp() {
        contentService = mock(ContentService.class);
        problemRepository = mock(ProblemRepository.class);
        spreadsheetService = mock(SpreadsheetService.class);
        service = new ProblemCleaningService(
                contentService, problemRepository, spreadsheetService, mock(BadWords.class));

        logger = (Logger) LoggerFactory.getLogger(ProblemCleaningService.class);
        logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(logs);
    }

    @Test
    void duplicateIsCleanedBeforeGoingToTheSpreadsheet() {
        when(contentService.cleanContent(RAW)).thenReturn(CLEANED);
        givenUncleaned(problem("1"), problem("2"));

        service.cleanProblems();

        ArgumentCaptor<Problem> duplicate = ArgumentCaptor.forClass(Problem.class);
        verify(spreadsheetService).logDuplicateComment(duplicate.capture());
        assertThat(duplicate.getValue().getId()).isEqualTo("2");
        assertThat(duplicate.getValue().getProblemDetails()).isEqualTo(CLEANED);
        assertNoCommentTextLogged();
    }

    @Test
    void failedCleaningDoesNotLogTheComment() {
        when(contentService.cleanContent(anyString())).thenThrow(new IllegalStateException("boom"));
        givenUncleaned(problem("1"));

        service.cleanProblems();

        assertThat(logs.list).anyMatch(e -> e.getFormattedMessage().contains("1"));
        assertNoCommentTextLogged();
    }

    private void givenUncleaned(Problem... problems) {
        when(problemRepository.findByPersonalInfoProcessed(null))
                .thenReturn(new ArrayList<>(List.of(problems)));
        when(problemRepository.findByPersonalInfoProcessed("false")).thenReturn(new ArrayList<>());
        when(problemRepository.save(any(Problem.class))).thenAnswer(i -> i.getArgument(0));
    }

    private void assertNoCommentTextLogged() {
        assertThat(logs.list).extracting(ILoggingEvent::getFormattedMessage)
                .noneMatch(m -> m.contains("John") || m.contains("613-555"));
    }

    private static Problem problem(String id) {
        Problem p = new Problem();
        p.setId(id);
        p.setProblemDetails(RAW);
        p.setUrl("https://www.canada.ca/en/services/benefits.html");
        return p;
    }
}
