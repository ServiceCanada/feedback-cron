package ca.gc.tbs.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import ca.gc.tbs.domain.Problem;
import ca.gc.tbs.repository.ProblemRepository;

class AirtableSyncServiceTest {

    private ProblemRepository problemRepository;
    private AirtableSyncService service;

    @BeforeEach
    void setUp() {
        problemRepository = mock(ProblemRepository.class);
        service = new AirtableSyncService(problemRepository, mock(SpreadsheetService.class));
    }

    @Test
    void prodOnlyCompletesProblemsSyncedToAirtable() {
        Problem synced = cleanedProblem("true");
        Problem notSynced = cleanedProblem(null);
        unprocessed(synced, notSynced);

        service.completeProcessing(true);

        assertEquals("true", synced.getProcessed());
        assertNull(notSynced.getProcessed());
    }

    @Test
    void localModeCompletesCleanedProblemsWithoutAirtable() {
        Problem cleaned = cleanedProblem(null);
        Problem notCleaned = new Problem();
        unprocessed(cleaned, notCleaned);

        service.completeProcessing(false);

        assertEquals("true", cleaned.getProcessed());
        assertNull(notCleaned.getProcessed());
    }

    @Test
    void initializeFailsWithoutAirtableSettings() {
        ReflectionTestUtils.setField(service, "airtableKey", "");
        ReflectionTestUtils.setField(service, "problemAirtableBase", "");
        ReflectionTestUtils.setField(service, "problemAirtableTab", "");

        assertThrows(IllegalStateException.class, service::initialize);
    }

    private Problem cleanedProblem(String airTableSync) {
        Problem problem = new Problem();
        problem.setPersonalInfoProcessed("true");
        problem.setAirTableSync(airTableSync);
        return problem;
    }

    private void unprocessed(Problem... problems) {
        when(problemRepository.findByProcessed("false")).thenReturn(new ArrayList<>(List.of(problems)));
        when(problemRepository.findByProcessed(null)).thenReturn(new ArrayList<>());
    }
}
